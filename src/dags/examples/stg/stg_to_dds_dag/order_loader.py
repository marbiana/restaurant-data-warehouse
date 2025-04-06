from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
from psycopg import Connection
from psycopg.rows import class_row
from lib.dict_util import str2json
from datetime import datetime
import logging
from lib import PgConnect
from examples.stg.stg_to_dds_dag.dds_settings_repository import DdsEtlSettingsRepository

log = logging.getLogger(__name__)

class OrderJsonObj(BaseModel):
    id: int
    object_id: str  # order_key из системы-источника.
    object_value: dict  # JSON с данными заказа.
    update_ts: datetime  # Время обновления заказа.

class OrderDdsObj(BaseModel):
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int

class OrderRawRepository:
    def load_raw_orders(self, conn: Connection) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                SELECT 
                    id,
                    object_id,
                    object_value::json AS object_value,
                    update_ts
                FROM stg.ordersystem_orders;
                """
            )
            return cur.fetchall()

class OrderDdsRepository:
    def insert_order(self, conn: Connection, order: OrderDdsObj) -> None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                """
                INSERT INTO dds.dm_orders(
                    order_key,
                    order_status,
                    restaurant_id,
                    timestamp_id,
                    user_id
                )
                VALUES (
                    %(order_key)s,
                    %(order_status)s,
                    %(restaurant_id)s,
                    %(timestamp_id)s,
                    %(user_id)s
                );
                """,
                order.dict()
            )
        except Exception as e:
            log.error("Ошибка вставки заказа в dm_orders: %s", str(e))

class OrderLoader:

    WF_KEY = "orders_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_order_id"

    def __init__(self, pg: PgConnect, settings_repo: DdsEtlSettingsRepository):
        self.dwh = pg
        self.raw = OrderRawRepository()
        self.dds = OrderDdsRepository()
        self.settings_repo = settings_repo

    def _get_restaurant_id(self, conn: Connection, source_restaurant_id: str, order_ts: datetime) -> Optional[int]:
        """Находит id ресторана в dm_restaurants по его исходному ID и времени заказа."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_restaurants 
                WHERE restaurant_id = %(source_id)s 
                ;
                """,
                {"source_id": source_restaurant_id}
            )
            result = cur.fetchone()
            return result[0] if result else None

    def _get_user_id(self, conn: Connection, source_user_id: str, order_ts: datetime) -> Optional[int]:
        """Находит id пользователя в dm_users по его исходному ID и времени заказа."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_users 
                WHERE user_id = %(source_id)s;
                """,
                {"source_id": source_user_id}
            )
            result = cur.fetchone()
            return result[0] if result else None

    def _get_timestamp_id(self, conn: Connection, order_ts: datetime) -> Optional[int]:
        """Находит id временной метки в dm_timestamps по точному времени."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_timestamps 
                WHERE ts = %(order_ts)s;
                """,
                {"order_ts": order_ts}
            )
            result = cur.fetchone()
            return result[0] if result else None

    def load_orders(self):
        """Основной метод загрузки данных в dm_orders."""
        with self.dwh.connection() as conn:
            raw_orders = self.raw.load_raw_orders(conn)
            for r in raw_orders:
                order_data = r.object_value
                try:
                    order_ts = datetime.fromisoformat(order_data["date"])
                except (KeyError, ValueError) as e:
                    log.error("Ошибка парсинга даты заказа %s: %s", r.object_id, str(e))
                    continue

                # Получение связанных ID
                restaurant_id = self._get_restaurant_id(conn, order_data["restaurant"]["id"], order_ts)
                user_id = self._get_user_id(conn, order_data["user"]["id"], order_ts)
                timestamp_id = self._get_timestamp_id(conn, order_ts)

                log.info("Полученные данные для заказа %s: restaurant_id=%s, user_id=%s, timestamp_id=%s", r.object_id, restaurant_id, user_id, timestamp_id)

                if not all([restaurant_id, user_id, timestamp_id]):
                    log.error("Не найдены связанные записи для заказа %s", r.object_id)
                    continue

                # Создание и сохранение объекта заказа
                order = OrderDdsObj(
                    order_key=r.object_id,
                    order_status=order_data["final_status"],
                    restaurant_id=restaurant_id,
                    timestamp_id=timestamp_id,
                    user_id=user_id
                )
                self.dds.insert_order(conn, order)
              