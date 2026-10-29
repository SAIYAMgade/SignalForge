from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session
from sqlalchemy import desc, func

from signalforge.db.connection import get_db
from signalforge.db.models import (
    DataSource, Pipeline, PipelineRun, RunStage,
    ValidationResult, RejectedRecord, SchemaVersion,
    SchemaDriftEvent, FailureDiagnostic, DataQualityRule,
    DimCustomer, FactTransaction
)
from signalforge.api.schemas import (
    DataSourceCreate, DataSourceResponse,
    PipelineCreate, PipelineResponse,
    PipelineRunResponse, PipelineRunDetailResponse,
    RunStageResponse, FailureDiagnosticResponse,
    QualityRuleCreate, QualityRuleResponse,
    ValidationResultResponse, RejectedRecordResponse,
    SchemaVersionResponse, SchemaDriftEventResponse,
    TriggerScenarioRequest, SystemSummaryResponse
)
from signalforge.engine.extractors import CSVExtractor, JSONExtractor, RESTAPIExtractor, PostgresExtractor, StreamExtractor
from signalforge.engine.pipeline import PipelineEngine
from signalforge.engine.transformers import Transformer
from signalforge.failure_lab.scenarios import FailureLab
from signalforge.observability.metrics import MetricsCollector

router = APIRouter()

# -------------------------------------------------------------
# Health & Readiness
# -------------------------------------------------------------
@router.get("/health", tags=["System"])
def health_check():
    return {"status": "ok", "service": "signalforge"}

@router.get("/ready", tags=["System"])
def readiness_check(db: Session = Depends(get_db)):
    try:
        db.execute(func.now())
        return {"status": "ready", "database": "connected"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {str(e)}"
        )

# -------------------------------------------------------------
# Data Sources
# -------------------------------------------------------------
@router.get("/api/sources", response_model=List[DataSourceResponse], tags=["Data Sources"])
def list_sources(db: Session = Depends(get_db)):
    return db.query(DataSource).order_by(DataSource.created_at.desc()).all()

@router.post("/api/sources", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED, tags=["Data Sources"])
def create_source(payload: DataSourceCreate, db: Session = Depends(get_db)):
    existing = db.query(DataSource).filter(DataSource.name == payload.name).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Source '{payload.name}' already exists.")
    source = DataSource(**payload.model_dump())
    db.add(source)
    db.commit()
    db.refresh(source)
    return source

@router.get("/api/sources/{source_id}", response_model=DataSourceResponse, tags=["Data Sources"])
def get_source(source_id: str, db: Session = Depends(get_db)):
    source = db.query(DataSource).filter(DataSource.id == source_id).first()
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")
    return source

# -------------------------------------------------------------
# Pipelines
# -------------------------------------------------------------
@router.get("/api/pipelines", response_model=List[PipelineResponse], tags=["Pipelines"])
def list_pipelines(db: Session = Depends(get_db)):
    return db.query(Pipeline).order_by(Pipeline.created_at.desc()).all()

@router.post("/api/pipelines", response_model=PipelineResponse, status_code=status.HTTP_201_CREATED, tags=["Pipelines"])
def create_pipeline(payload: PipelineCreate, db: Session = Depends(get_db)):
    existing = db.query(Pipeline).filter(Pipeline.name == payload.name).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Pipeline '{payload.name}' already exists.")
    source = db.query(DataSource).filter(DataSource.id == payload.source_id).first()
    if not source:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Referenced source_id does not exist.")
    pipeline = Pipeline(**payload.model_dump())
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    return pipeline

@router.get("/api/pipelines/{pipeline_id}", response_model=PipelineResponse, tags=["Pipelines"])
def get_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found.")
    return pipeline

@router.post("/api/pipelines/{pipeline_id}/run", response_model=PipelineRunResponse, tags=["Pipelines"])
def trigger_pipeline_run(pipeline_id: str, db: Session = Depends(get_db)):
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found.")

    source = pipeline.source
    cfg = source.config_json or {}

    # Build appropriate extractor based on source type
    if source.source_type == "CSV":
        file_path = cfg.get("path", f"data/{pipeline.target_table}.csv")
        extractor = CSVExtractor(file_path, cursor_column=cfg.get("cursor_column"))
    elif source.source_type == "JSON":
        file_path = cfg.get("path", f"data/{pipeline.target_table}.json")
        extractor = JSONExtractor(file_path, is_lines=cfg.get("is_lines", False), cursor_column=cfg.get("cursor_column"))
    elif source.source_type == "REST_API":
        extractor = RESTAPIExtractor(
            endpoint_url=cfg.get("endpoint_url", "http://localhost:8000/api/mock_data"),
            method=cfg.get("method", "GET"),
            headers=cfg.get("headers", {}),
            cursor_param=cfg.get("cursor_param")
        )
    elif source.source_type == "STREAM":
        events = cfg.get("events", [])
        extractor = StreamExtractor(events)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported source type: {source.source_type}")

    # Load active data quality rules for this target table
    rules_entities = db.query(DataQualityRule).filter(
        DataQualityRule.dataset_name == pipeline.target_table,
        DataQualityRule.is_active.is_(True)
    ).all()

    from signalforge.quality.rules import QualityRule, RuleSeverity, RuleType
    rules = [
        QualityRule(
            rule_name=r.rule_name,
            rule_type=RuleType(r.rule_type),
            dataset_name=r.dataset_name,
            params=r.params_json,
            severity=RuleSeverity(r.severity),
            id=r.id,
            is_active=r.is_active
        )
        for r in rules_entities
    ]

    engine = PipelineEngine(
        db=db,
        pipeline=pipeline,
        extractor=extractor,
        quality_rules=rules,
        dataset_name=pipeline.target_table,
        identifier_column=cfg.get("identifier_column", "customer_id" if pipeline.target_table == "dim_customers" else "transaction_id")
    )

    run = engine.run(trigger_type="MANUAL")
    return run

# -------------------------------------------------------------
# Pipeline Runs
# -------------------------------------------------------------
@router.get("/api/runs", response_model=List[PipelineRunResponse], tags=["Runs"])
def list_runs(
    pipeline_id: Optional[str] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    query = db.query(PipelineRun)
    if pipeline_id:
        query = query.filter(PipelineRun.pipeline_id == pipeline_id)
    if status_filter:
        query = query.filter(PipelineRun.status == status_filter.upper())
    return query.order_by(PipelineRun.started_at.desc()).offset(offset).limit(limit).all()

@router.get("/api/runs/{run_id}", response_model=PipelineRunDetailResponse, tags=["Runs"])
def get_run_details(run_id: str, db: Session = Depends(get_db)):
    run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found.")

    stages = db.query(RunStage).filter(RunStage.run_id == run_id).order_by(RunStage.started_at.asc()).all()
    diag = db.query(FailureDiagnostic).filter(FailureDiagnostic.run_id == run_id).first()

    diag_resp = None
    if diag:
        diag_resp = FailureDiagnosticResponse(
            id=diag.id,
            failing_stage=diag.failing_stage,
            error_category=diag.error_category,
            facts=diag.facts_json or [],
            possible_causes=diag.possible_causes_json or [],
            recommended_steps=diag.recommended_steps_json or [],
            created_at=diag.created_at
        )

    return PipelineRunDetailResponse(
        id=run.id,
        pipeline_id=run.pipeline_id,
        trigger_type=run.trigger_type,
        status=run.status,
        records_extracted=run.records_extracted,
        records_transformed=run.records_transformed,
        records_rejected=run.records_rejected,
        records_loaded=run.records_loaded,
        error_message=run.error_message,
        error_category=run.error_category,
        airflow_run_id=run.airflow_run_id,
        duration_ms=run.duration_ms,
        started_at=run.started_at,
        completed_at=run.completed_at,
        stages=stages,
        diagnostics=diag_resp
    )

@router.get("/api/runs/{run_id}/diagnostics", tags=["Runs"])
def get_run_diagnostics(run_id: str, db: Session = Depends(get_db)):
    diag = db.query(FailureDiagnostic).filter(FailureDiagnostic.run_id == run_id).first()
    if not diag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Diagnostic report not found for run.")
    return diag

@router.get("/api/runs/{run_id}/rejections", response_model=List[RejectedRecordResponse], tags=["Runs"])
def get_run_rejections(run_id: str, db: Session = Depends(get_db)):
    return db.query(RejectedRecord).filter(RejectedRecord.run_id == run_id).order_by(RejectedRecord.created_at.asc()).all()

# -------------------------------------------------------------
# Data Quality
# -------------------------------------------------------------
@router.get("/api/quality/rules", response_model=List[QualityRuleResponse], tags=["Data Quality"])
def list_quality_rules(dataset_name: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(DataQualityRule)
    if dataset_name:
        query = query.filter(DataQualityRule.dataset_name == dataset_name)
    return query.order_by(DataQualityRule.dataset_name.asc(), DataQualityRule.rule_name.asc()).all()

@router.post("/api/quality/rules", response_model=QualityRuleResponse, status_code=status.HTTP_201_CREATED, tags=["Data Quality"])
def create_quality_rule(payload: QualityRuleCreate, db: Session = Depends(get_db)):
    rule = DataQualityRule(**payload.model_dump())
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule

@router.get("/api/quality/results", response_model=List[ValidationResultResponse], tags=["Data Quality"])
def list_validation_results(run_id: Optional[str] = None, dataset_name: Optional[str] = None, limit: int = 50, db: Session = Depends(get_db)):
    query = db.query(ValidationResult)
    if run_id:
        query = query.filter(ValidationResult.run_id == run_id)
    if dataset_name:
        query = query.filter(ValidationResult.dataset_name == dataset_name)
    return query.order_by(ValidationResult.created_at.desc()).limit(limit).all()

# -------------------------------------------------------------
# Schemas & Drift
# -------------------------------------------------------------
@router.get("/api/schemas", response_model=List[SchemaVersionResponse], tags=["Schema Management"])
def list_schemas(db: Session = Depends(get_db)):
    return db.query(SchemaVersion).order_by(SchemaVersion.dataset_name.asc(), SchemaVersion.version.desc()).all()

@router.get("/api/schemas/{dataset_name}/versions", response_model=List[SchemaVersionResponse], tags=["Schema Management"])
def get_dataset_versions(dataset_name: str, db: Session = Depends(get_db)):
    return db.query(SchemaVersion).filter(SchemaVersion.dataset_name == dataset_name).order_by(SchemaVersion.version.desc()).all()

@router.get("/api/schemas/drift", response_model=List[SchemaDriftEventResponse], tags=["Schema Management"])
def list_drift_events(dataset_name: Optional[str] = None, limit: int = 50, db: Session = Depends(get_db)):
    query = db.query(SchemaDriftEvent)
    if dataset_name:
        query = query.filter(SchemaDriftEvent.dataset_name == dataset_name)
    return query.order_by(SchemaDriftEvent.created_at.desc()).limit(limit).all()

# -------------------------------------------------------------
# Failure Recovery Laboratory
# -------------------------------------------------------------
@router.get("/api/failure-lab/scenarios", tags=["Failure Laboratory"])
def get_failure_scenarios(db: Session = Depends(get_db)):
    lab = FailureLab(db)
    return lab.list_scenarios()

@router.post("/api/failure-lab/trigger", tags=["Failure Laboratory"])
def trigger_failure_scenario(payload: TriggerScenarioRequest, db: Session = Depends(get_db)):
    lab = FailureLab(db)
    try:
        return lab.run_scenario(payload.scenario_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Scenario execution failed: {str(e)}")

# -------------------------------------------------------------
# Operational Summary & Metrics
# -------------------------------------------------------------
@router.get("/api/metrics/summary", response_model=SystemSummaryResponse, tags=["Observability"])
def get_operational_summary(db: Session = Depends(get_db)):
    total_pipelines = db.query(func.count(Pipeline.id)).scalar() or 0
    active_pipelines = db.query(func.count(Pipeline.id)).filter(Pipeline.is_active.is_(True)).scalar() or 0

    total_runs = db.query(func.count(PipelineRun.id)).scalar() or 0
    successful_runs = db.query(func.count(PipelineRun.id)).filter(PipelineRun.status == "COMPLETED").scalar() or 0
    failed_runs = db.query(func.count(PipelineRun.id)).filter(PipelineRun.status == "FAILED").scalar() or 0

    failure_rate = round((failed_runs / total_runs * 100), 2) if total_runs > 0 else 0.0

    total_ingested = db.query(func.sum(PipelineRun.records_loaded)).scalar() or 0
    total_rejected = db.query(func.sum(PipelineRun.records_rejected)).scalar() or 0

    # Recent failure incidents
    failed_run_entities = (
        db.query(PipelineRun)
        .filter(PipelineRun.status == "FAILED")
        .order_by(PipelineRun.completed_at.desc())
        .limit(5)
        .all()
    )

    incidents = []
    for fr in failed_run_entities:
        diag = fr.diagnostics
        incidents.append({
            "run_id": fr.id,
            "pipeline_name": fr.pipeline.name if fr.pipeline else "Unknown",
            "error_category": fr.error_category or "UNKNOWN",
            "error_message": fr.error_message,
            "failed_at": fr.completed_at.isoformat() if fr.completed_at else fr.started_at.isoformat(),
            "failing_stage": diag.failing_stage if diag else "UNKNOWN",
            "recommended_steps": diag.recommended_steps_json if diag else []
        })

    return SystemSummaryResponse(
        total_pipelines=total_pipelines,
        active_pipelines=active_pipelines,
        total_runs=total_runs,
        successful_runs=successful_runs,
        failed_runs=failed_runs,
        failure_rate_percent=failure_rate,
        total_records_ingested=int(total_ingested),
        total_records_rejected=int(total_rejected),
        recent_incidents=incidents
    )

# -------------------------------------------------------------
# Prometheus Metrics Exposition
# -------------------------------------------------------------
@router.get("/metrics", tags=["Observability"])
def get_prometheus_metrics():
    payload, content_type = MetricsCollector.get_metrics_payload()
    return Response(content=payload, media_type=content_type)
