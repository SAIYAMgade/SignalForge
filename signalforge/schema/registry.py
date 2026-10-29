from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session
from signalforge.db.models import SchemaVersion, SchemaDriftEvent
from signalforge.schema.drift import DriftReport, SchemaDriftDetector, infer_schema_from_df

class SchemaRegistry:
    """Manages dataset schema versions and records detected drift events."""

    def __init__(self, db: Session):
        self.db = db

    def register_version(
        self,
        dataset_name: str,
        columns_meta: Dict[str, Dict[str, Any]],
        activate: bool = True
    ) -> SchemaVersion:
        latest = (
            self.db.query(SchemaVersion)
            .filter(SchemaVersion.dataset_name == dataset_name)
            .order_by(SchemaVersion.version.desc())
            .first()
        )
        new_version = (latest.version + 1) if latest else 1

        if activate and latest:
            self.db.query(SchemaVersion).filter(
                SchemaVersion.dataset_name == dataset_name
            ).update({"is_active": False})

        schema_obj = SchemaVersion(
            dataset_name=dataset_name,
            version=new_version,
            columns_json=columns_meta,
            is_active=activate
        )
        self.db.add(schema_obj)
        self.db.commit()
        self.db.refresh(schema_obj)
        return schema_obj

    def get_active_schema(self, dataset_name: str) -> Optional[SchemaVersion]:
        return (
            self.db.query(SchemaVersion)
            .filter(
                SchemaVersion.dataset_name == dataset_name,
                SchemaVersion.is_active.is_(True)
            )
            .first()
        )

    def check_and_record_drift(
        self,
        dataset_name: str,
        observed_schema: Dict[str, Dict[str, Any]],
        pipeline_id: Optional[str] = None,
        run_id: Optional[str] = None
    ) -> Optional[DriftReport]:
        active_version = self.get_active_schema(dataset_name)
        if not active_version:
            # First time observing this dataset: automatically register version 1
            self.register_version(dataset_name, observed_schema, activate=True)
            return None

        baseline_columns = active_version.columns_json
        report = SchemaDriftDetector.detect_drift(baseline_columns, observed_schema)

        if report.has_drift:
            event = SchemaDriftEvent(
                pipeline_id=pipeline_id,
                run_id=run_id,
                dataset_name=dataset_name,
                baseline_version=active_version.version,
                drift_type=",".join([t.value for t in report.drift_types]),
                severity=report.severity.value,
                diff_json=report.diff,
                recommended_action=report.recommended_action
            )
            self.db.add(event)
            self.db.commit()

        return report
