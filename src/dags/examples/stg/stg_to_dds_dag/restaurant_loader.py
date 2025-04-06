from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
from psycopg import Connection
from psycopg.rows import class_row
from lib.dict_util import json2str, str2json
from examples.stg.stg_to_dds_dag.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect


class RestaurantJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str

class RestaurantDdsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime = datetime(2099, 12, 31)

class RestaurantRawRepository:
    def load_raw_restaurants(self, conn: Connection, last_loaded_id: int) -> List[RestaurantJsonObj]:
        with conn.cursor(row_factory=class_row(RestaurantJsonObj)) as cur:
            cur.execute(
                """
                SELECT
                    id,
                    object_id,
                    object_value
                FROM stg.ordersystem_restaurants
                WHERE id > %(last_loaded_id)s;
                """,
                {"last_loaded_id": last_loaded_id},
            )
            return cur.fetchall()

class RestaurantDdsRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_restaurants(
                    restaurant_id,
                    restaurant_name,
                    active_from,
                    active_to
                )
                VALUES (
                    %(restaurant_id)s,
                    %(restaurant_name)s,
                    %(active_from)s,
                    %(active_to)s
                );
                """,
                restaurant.dict(exclude={"id"}),
            )

    def close_old_record(self, conn: Connection, restaurant_id: str, active_to: datetime) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dds.dm_restaurants
                SET active_to = %(active_to)s
                WHERE restaurant_id = %(restaurant_id)s
                  AND active_to = '2099-12-31'::timestamp;
                """,
                {"restaurant_id": restaurant_id, "active_to": active_to},
            )    
class RestaurantLoader:
    WF_KEY = "restaurants_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_restaurant_id"

    def __init__(self, pg: PgConnect, settings_repo: DdsEtlSettingsRepository):
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds = RestaurantDdsRepository()
        self.settings_repo = settings_repo

    def parse_restaurants(self, raws: List[RestaurantJsonObj]) -> List[RestaurantDdsObj]:
        res = []
        for r in raws:
            data = str2json(r.object_value)
            res.append(
                RestaurantDdsObj(
                    id=r.id,
                    restaurant_id=r.object_id,
                    restaurant_name=data["name"],
                    active_from=data["update_ts"]
                )
            )
        return res

    def load_restaurants(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repo.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.raw.load_raw_restaurants(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)

            restaurants = self.parse_restaurants(load_queue)
            for rest in restaurants:
                self.dds.close_old_record(conn, rest.restaurant_id, rest.active_from)
                self.dds.insert_restaurant(conn, rest)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = rest.id
                self.settings_repo.save_setting(conn, wf_setting)
