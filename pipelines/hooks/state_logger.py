from abc import ABC, abstractmethod
from typing import Dict, Callable, Any
from datetime import datetime, timezone

from psycopg2 import sql
from prefect.context import get_run_context

from pipelines.utils.db import get_db_connection


class LogStrategy(ABC):
    @abstractmethod
    def write(self, payload: Dict):
        pass


class PostgresLog(LogStrategy):
    def __init__(
        self,
        conn_factory: Callable[[], Any],
        table_name: str,
    ):
        self.conn_factory = conn_factory
        self.table_name = table_name

    def _ensure_table(self, cursor):
        cursor.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    log_id TEXT PRIMARY KEY,
                    flow_name TEXT NOT NULL,
                    flow_run_id TEXT NOT NULL,
                    job_name TEXT,
                    state TEXT NOT NULL,
                    message TEXT,
                    timestamp TIMESTAMPTZ NOT NULL
                )
            """).format(sql.Identifier(self.table_name))
        )
        cursor.execute(
            sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {} (flow_run_id, timestamp)
            """).format(
                sql.Identifier(f"{self.table_name}_flow_run_timestamp_idx"),
                sql.Identifier(self.table_name),
            )
        )

    def write(self, payload: Dict[str, Any]):
        conn = self.conn_factory()

        try:
            with conn.cursor() as cursor:
                self._ensure_table(cursor)

                columns = sql.SQL(", ").join(
                    sql.Identifier(column) for column in payload.keys()
                )
                placeholders = sql.SQL(", ").join(
                    sql.Placeholder() for _ in payload
                )
                query = sql.SQL("""
                    INSERT INTO {}
                    ({})
                    VALUES
                    ({})
                """).format(
                    sql.Identifier(self.table_name),
                    columns,
                    placeholders,
                )
                cursor.execute(query, list(payload.values()))

            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"[LOGGER ERROR] failed to log state: {e}")
        finally:
            if conn:
                conn.close()


class LoggerFactory:
    @staticmethod
    def create(log_db: str, **kwargs) -> LogStrategy:
        if log_db == "postgres":
            conn_factory = kwargs.get("conn_factory")
            if not conn_factory:
                raise ValueError("Value 'conn_factory' required for Postgres logger")
            table_name = kwargs.get("table_name", "prefect_logs")
            return PostgresLog(conn_factory, table_name)
        else:
            raise ValueError(f"Unknown logger database type: {log_db}")


def _write_state_log(flow, flow_run, state_name: str, message: str):
    params = flow_run.parameters
    job_name = params["job_name"]
    flow_run_id = str(flow_run.id)
    flow_name = flow.name
    timestamp = datetime.now(timezone.utc)
    log_id = f"{flow_run_id}-{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}"

    try:
        logger = LoggerFactory.create(
            log_db=params["log_db"],
            conn_factory=get_db_connection,
        )

        payload = {
            "log_id": log_id,
            "flow_name": flow_name,
            "flow_run_id": flow_run_id,
            "job_name": job_name,
            "state": state_name,
            "message": message,
            "timestamp": timestamp,
        }

        logger.write(payload)
    except Exception as e:
        print(f"[LOGGER ERROR] failed to log state for job {job_name}: {e}")


def log_flow_start():
    context = get_run_context()

    _write_state_log(
        flow=context.flow,
        flow_run=context.flow_run,
        state_name="Started",
        message="Flow execution initiated",
    )


def state_log_hook(flow, flow_run, state):
    state_name = state.name
    message = state.message

    if state.is_failed():
        try:
            result = state.result(raise_on_failure=False)
            if isinstance(result, Exception):
                message = f"Error Type: {type(result).__name__} | Details: {str(result)}"
        except Exception as e:
            message = f"Failed to extract exception details: {e}"

    _write_state_log(
        flow=flow,
        flow_run=flow_run,
        state_name=state_name,
        message=message
    )
