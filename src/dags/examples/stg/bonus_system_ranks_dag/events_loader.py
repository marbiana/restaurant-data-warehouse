from logging import Logger
from typing import List, Optional
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
import json

class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str

class EventEtlSetting(BaseModel):
    id: int
    workflow_key: str
    last_loaded_id: int

class EventEtlSettingsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_setting(self, workflow_key: str) -> Optional[EventEtlSetting]:
        with self._db.client().cursor(row_factory=class_row(EventEtlSetting)) as cur:
            cur.execute(
                """
                SELECT id, workflow_key, 
                       (workflow_settings->>'last_loaded_id')::int as last_loaded_id
                FROM stg.srv_wf_settings
                WHERE workflow_key = %(workflow_key)s;
                """,
                {"workflow_key": workflow_key},
            )
            obj = cur.fetchone()
        return obj

    def save_setting(self, workflow_key: str, last_loaded_id: int) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES (%(workflow_key)s, %(workflow_settings)s::jsonb)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    {
                        "workflow_key": workflow_key,
                        "workflow_settings": json.dumps({"last_loaded_id": last_loaded_id})
                    },
                )
                conn.commit()

class EventsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, last_loaded_id: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                SELECT 
                    id, 
                    event_ts, 
                    event_type, 
                    event_value
                FROM public.outbox
                WHERE id > %(last_loaded_id)s
                ORDER BY id;
                """,
                {"last_loaded_id": last_loaded_id},
            )
            objs = cur.fetchall()
        return objs

class EventDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_events(self, events: List[EventObj]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for event in events:
                    cur.execute(
                        """
                        INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                        OVERRIDING SYSTEM VALUE
                        VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                        ON CONFLICT (id) DO NOTHING;
                        """,
                        {
                            "id": event.id,
                            "event_ts": event.event_ts,
                            "event_type": event.event_type,
                            "event_value": event.event_value
                        },
                    )
                conn.commit()

class EventLoader:
    WF_KEY = "events_origin_to_stg_workflow"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.origin = EventsOriginRepository(pg_origin)
        self.stg = EventDestRepository(pg_dest)
        self.settings_repository = EventEtlSettingsRepository(pg_dest)
        self.log = log

    def load_events(self):
        # 1. Получаем последнее состояние загрузки
        setting = self.settings_repository.get_setting(self.WF_KEY)
        if not setting:
            self.log.info("No previous loading found. Initializing with last_loaded_id = 0")
            self.settings_repository.save_setting(self.WF_KEY, 0)
            last_loaded_id = 0
        else:
            last_loaded_id = setting.last_loaded_id

        self.log.info(f"Last loaded ID: {last_loaded_id}")

        # 2. Получаем новые записи из outbox
        load_queue = self.origin.list_events(last_loaded_id)
        self.log.info(f"Found {len(load_queue)} new events to load")

        if not load_queue:
            self.log.info("No new data to load")
            return

        # 3. Загружаем данные в stg.bonussystem_events
        self.stg.insert_events(load_queue)
        self.log.info(f"Loaded {len(load_queue)} events to stg.bonussystem_events")

        # 4. Сохраняем новое состояние
        new_last_loaded_id = max(event.id for event in load_queue)
        self.settings_repository.save_setting(self.WF_KEY, new_last_loaded_id)
        self.log.info(f"Updated last_loaded_id to {new_last_loaded_id}")