from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
from psycopg import Connection
from psycopg.rows import class_row
from lib.dict_util import str2json
from examples.stg.stg_to_dds_dag.dds_settings_repository import DdsEtlSettingsRepository
from lib import PgConnect


class ProductJsonObj(BaseModel):
    id: int
    object_id: str  # ID ресторана (restaurant_id).
    object_value: dict  # JSON с меню.
    update_ts: datetime  # Время обновления записи ресторана.

class ProductDdsObj(BaseModel):
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime  # Берется из update_ts таблицы ordersystem_restaurants.
    active_to: datetime = datetime(2099, 12, 31)
    restaurant_id: int  # Ссылка на dds.dm_restaurants.id.

class ProductRawRepository:
    def load_raw_products(self, conn: Connection) -> List[ProductJsonObj]:
        with conn.cursor(row_factory=class_row(ProductJsonObj)) as cur:
            cur.execute(
                """
                SELECT 
                    id,
                    object_id,  -- restaurant_id из системы-источника.
                    object_value::json AS object_value,  -- JSON меню.
                    update_ts  -- Время обновления ресторана.
                FROM stg.ordersystem_restaurants;
                """
            )
            return cur.fetchall()

class ProductDdsRepository:
    def insert_product(self, conn: Connection, product: ProductDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_products(
                    product_id,
                    product_name,
                    product_price,
                    active_from,
                    active_to,
                    restaurant_id
                )
                VALUES (
                    %(product_id)s,
                    %(product_name)s,
                    %(product_price)s,
                    %(active_from)s,
                    %(active_to)s,
                    %(restaurant_id)s
                )
                ON CONFLICT ON CONSTRAINT unique_product_history DO NOTHING;  -- Используем имя ограничения!
                """,
                product.dict(),
            )

    def close_old_record(self, conn: Connection, product_id: str, active_to: datetime) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dds.dm_products
                SET active_to = %(active_to)s
                WHERE product_id = %(product_id)s
                  AND active_to = '2099-12-31'::timestamp;
                """,
                {"product_id": product_id, "active_to": active_to},
            )    

class ProductLoader:

    WF_KEY = "products_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_product_id"

    def __init__(self, pg: PgConnect, settings_repo: DdsEtlSettingsRepository):
        self.dwh = pg
        self.raw = ProductRawRepository()
        self.dds = ProductDdsRepository()
        self.settings_repo = settings_repo

    def parse_products(self, raws: List[ProductJsonObj], restaurant_mapping: dict) -> List[ProductDdsObj]:
        res = []
        for r in raws:
            # update_ts берется из колонки таблицы, а не из JSON.
            try:
                active_from = r.update_ts  # Уже datetime (если в БД timestamp).
            except Exception as e:
                log.error("Ошибка в update_ts ресторана %s: %s", r.object_id, str(e))
                continue

            # Парсим меню из JSON.
            menu = r.object_value.get("menu", [])
            for product in menu:
                if not all(key in product for key in ["_id", "name", "price"]):
                    log.error("Некорректный продукт: %s", product)
                    continue

                res.append(
                    ProductDdsObj(
                        product_id=product["_id"],
                        product_name=product["name"],
                        product_price=product["price"],
                        active_from=active_from,
                        restaurant_id=restaurant_mapping.get(r.object_id, -1)  # object_id = restaurant_id из STG.
                    )
                )
        return res

    def load_products(self):
        with self.dwh.connection() as conn:
            # Получаем маппинг ресторанов: {restaurant_source_id: dds_id}
            restaurant_mapping = self._get_restaurant_mapping(conn)
            
            raw_data = self.raw.load_raw_products(conn)
            products = self.parse_products(raw_data, restaurant_mapping)
            
            for product in products:
                self.dds.close_old_record(conn, product.product_id, product.active_from)
                self.dds.insert_product(conn, product)

    def _get_restaurant_mapping(self, conn: Connection) -> dict:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT restaurant_id, id FROM dds.dm_restaurants;"
            )
            return {row[0]: row[1] for row in cur.fetchall()}