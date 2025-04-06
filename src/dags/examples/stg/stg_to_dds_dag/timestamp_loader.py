from pydantic import BaseModel
from datetime import datetime, time, date
from typing import Optional
from typing import List
from psycopg import Connection
from psycopg.rows import class_row
from lib.dict_util import str2json
from datetime import datetime
from examples.stg.stg_to_dds_dag.dds_settings_repository import DdsEtlSettingsRepository
from lib import PgConnect

class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str

class TimestampDdsObj(BaseModel):
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time

class TimestampRawRepository:
    def load_raw_timestamps(self, conn: Connection) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                SELECT 
                    id, 
                    object_id, 
                    object_value 
                FROM stg.ordersystem_orders 
                WHERE object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED');
                """
            )
            return cur.fetchall()

class TimestampDdsRepository:
    def insert_timestamp(self, conn: Connection, timestamp: TimestampDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                ON CONFLICT (ts) DO NOTHING;
                """,
                timestamp.dict()
            )
class TimestampLoader:
    WF_KEY = "timestamps_raw_to_dds_workflow"

    def __init__(self, pg: PgConnect, settings_repo: DdsEtlSettingsRepository):
        self.dwh = pg
        self.raw = TimestampRawRepository()
        self.dds = TimestampDdsRepository()
        self.settings_repo = settings_repo

    def parse_timestamps(self, raws: List[OrderJsonObj]) -> List[TimestampDdsObj]:
        res = []
        for r in raws:
            order_data = str2json(r.object_value)
            date_str = order_data.get("date")
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str)
                    res.append(
                        TimestampDdsObj(
                            ts=dt,
                            year=dt.year,
                            month=dt.month,
                            day=dt.day,
                            date=dt.date(),
                            time=dt.time()
                        )
                    )
                except ValueError:
                    continue
        return res

    def load_timestamps(self):
        with self.dwh.connection() as conn:
            raw_orders = self.raw.load_raw_timestamps(conn)
            timestamps = self.parse_timestamps(raw_orders)
            for ts in timestamps:
                self.dds.insert_timestamp(conn, ts)            