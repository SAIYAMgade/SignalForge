import json
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

# Return type: (DataFrame, new_checkpoint_value)
Tuple_Data = tuple[pd.DataFrame, Optional[str]]

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, checkpoint_value: Optional[str] = None) -> Tuple_Data:
        pass

class CSVExtractor(BaseExtractor):
    def __init__(self, file_path: str, delimiter: str = ",", cursor_column: Optional[str] = None):
        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.cursor_column = cursor_column

    def extract(self, checkpoint_value: Optional[str] = None) -> Tuple_Data:
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV source file not found: {self.file_path}")

        df = pd.read_csv(self.file_path, delimiter=self.delimiter)
        new_checkpoint = checkpoint_value

        if self.cursor_column and self.cursor_column in df.columns:
            if checkpoint_value:
                df = df[df[self.cursor_column].astype(str) > str(checkpoint_value)]
            if not df.empty:
                new_checkpoint = str(df[self.cursor_column].max())

        return df, new_checkpoint


class JSONExtractor(BaseExtractor):
    def __init__(self, file_path: str, is_lines: bool = False, cursor_column: Optional[str] = None):
        self.file_path = Path(file_path)
        self.is_lines = is_lines
        self.cursor_column = cursor_column

    def extract(self, checkpoint_value: Optional[str] = None) -> Tuple_Data:
        if not self.file_path.exists():
            raise FileNotFoundError(f"JSON source file not found: {self.file_path}")

        if self.is_lines:
            df = pd.read_json(self.file_path, lines=True)
        else:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.DataFrame(data)

        new_checkpoint = checkpoint_value
        if self.cursor_column and self.cursor_column in df.columns:
            if checkpoint_value:
                df = df[df[self.cursor_column].astype(str) > str(checkpoint_value)]
            if not df.empty:
                new_checkpoint = str(df[self.cursor_column].max())

        return df, new_checkpoint


class RESTAPIExtractor(BaseExtractor):
    def __init__(
        self,
        endpoint_url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        timeout_seconds: float = 5.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        cursor_param: Optional[str] = None
    ):
        self.endpoint_url = endpoint_url
        self.method = method.upper()
        self.headers = headers or {}
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.cursor_param = cursor_param

    def extract(self, checkpoint_value: Optional[str] = None) -> Tuple_Data:
        params = {}
        if self.cursor_param and checkpoint_value:
            params[self.cursor_param] = checkpoint_value

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout_seconds) as client:
                    if self.method == "GET":
                        resp = client.get(self.endpoint_url, headers=self.headers, params=params)
                    else:
                        resp = client.post(self.endpoint_url, headers=self.headers, json=params)

                    if resp.status_code >= 500:
                        raise httpx.HTTPStatusError(
                            f"Server Error {resp.status_code}", request=resp.request, response=resp
                        )
                    resp.raise_for_status()

                    data = resp.json()
                    records = data if isinstance(data, list) else data.get("records", data.get("items", [data]))
                    df = pd.DataFrame(records)

                    new_checkpoint = checkpoint_value
                    if self.cursor_param and not df.empty and self.cursor_param in df.columns:
                        new_checkpoint = str(df[self.cursor_param].max())

                    return df, new_checkpoint

            except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as e:
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(self.backoff_factor * (2 ** (attempt - 1)))
                else:
                    raise RuntimeError(
                        f"REST API extraction failed after {self.max_retries} attempts to {self.endpoint_url}: {str(e)}"
                    ) from e

        raise RuntimeError(f"REST API extraction failed: {str(last_error)}")


class PostgresExtractor(BaseExtractor):
    def __init__(self, query: str, db_session: Session, cursor_column: Optional[str] = None):
        self.query = query
        self.db_session = db_session
        self.cursor_column = cursor_column

    def extract(self, checkpoint_value: Optional[str] = None) -> Tuple_Data:
        sql = self.query
        params = {}
        if self.cursor_column and checkpoint_value:
            sql += f" WHERE {self.cursor_column} > :cursor_val ORDER BY {self.cursor_column} ASC"
            params["cursor_val"] = checkpoint_value

        result = self.db_session.execute(text(sql), params)
        keys = result.keys()
        rows = [dict(zip(keys, row)) for row in result.fetchall()]
        df = pd.DataFrame(rows)

        new_checkpoint = checkpoint_value
        if self.cursor_column and not df.empty and self.cursor_column in df.columns:
            new_checkpoint = str(df[self.cursor_column].max())

        return df, new_checkpoint


class StreamExtractor(BaseExtractor):
    """Simulates real-time event ingestion by popping batches from an event log."""
    def __init__(self, events: List[Dict[str, Any]], batch_size: int = 100):
        self.events = events
        self.batch_size = batch_size

    def extract(self, checkpoint_value: Optional[str] = None) -> Tuple_Data:
        start_idx = int(checkpoint_value) if checkpoint_value and checkpoint_value.isdigit() else 0
        end_idx = min(start_idx + self.batch_size, len(self.events))

        batch = self.events[start_idx:end_idx]
        df = pd.DataFrame(batch)
        new_checkpoint = str(end_idx)

        return df, new_checkpoint
