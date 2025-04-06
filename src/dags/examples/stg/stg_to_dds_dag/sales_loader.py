from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict
from typing import List, Optional
from typing import List
from psycopg import Connection
from psycopg.rows import class_row
from lib.dict_util import str2json
from datetime import datetime
import logging
from psycopg import Connection
from lib import PgConnect
from examples.stg.stg_to_dds_dag.dds_settings_repository import DdsEtlSettingsRepository

log = logging.getLogger(__name__)


class BonusTransactionObj(BaseModel):
    event_ts: datetime
    event_type: str
    event_value: dict  # JSON с данными о транзакции.

class ProductSaleDdsObj(BaseModel):
    product_id: int    # ID из dm_products (не product_id из STG!).
    order_id: int      # ID из dm_orders.
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class SalesRawRepository:
    def load_raw_transactions(self, conn: Connection) -> List[BonusTransactionObj]:
        with conn.cursor(row_factory=class_row(BonusTransactionObj)) as cur:
            cur.execute(
                """
                SELECT 
                    event_ts,
                    event_type,
                    event_value::json AS event_value
                FROM stg.bonussystem_events 
                WHERE event_type = 'bonus_transaction';
                """
            )
            return cur.fetchall()

class SalesDdsRepository:
    def insert_sale(self, conn: Connection, sale: ProductSaleDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.fct_product_sales(
                    product_id,
                    order_id,
                    count,
                    price,
                    total_sum,
                    bonus_payment,
                    bonus_grant
                )
                VALUES (
                    %(product_id)s,
                    %(order_id)s,
                    %(count)s,
                    %(price)s,
                    %(total_sum)s,
                    %(bonus_payment)s,
                    %(bonus_grant)s
                );
                """,
                sale.dict()
            )

from datetime import datetime
import logging
from psycopg import Connection

log = logging.getLogger(__name__)

class SalesLoader:
    def __init__(self, pg: PgConnect, settings_repo: DdsEtlSettingsRepository):
        self.dwh = pg
        self.raw = SalesRawRepository()
        self.dds = SalesDdsRepository()
        self.settings_repo = settings_repo
    

    def _get_order_id(self, conn: Connection, order_key: str) -> Optional[int]:
        """Находит order_id по ключу заказа."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM dds.dm_orders WHERE order_key = %(key)s;",
                {"key": order_key}
            )
            result = cur.fetchone()
            return result[0] if result else None

    def _get_product_id(self, conn: Connection, product_source_id: str, order_ts: datetime) -> Optional[int]:
        """Находит актуальный product_id на момент заказа."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_products 
                WHERE product_id = %(source_id)s 
                ;
                """,
                {"source_id": product_source_id}
            )
            result = cur.fetchone()
            return result[0] if result else None
      

    def load_sales(self):
        """Основной метод загрузки фактов продаж."""
        with self.dwh.connection() as conn:
            raw_transactions = self.raw.load_raw_transactions(conn)
            for tx in raw_transactions:
                try:
                    data = tx.event_value
                    order_key = data["order_id"]
                    order_ts = datetime.fromisoformat(data["order_date"])
                    
                    # Получаем order_id
                    order_id = self._get_order_id(conn, order_key)
                    if not order_id:
                        log.error("Заказ %s не найден в dm_orders.", order_key)
                        continue

                    # Обрабатываем каждый продукт в транзакции
                    for product in data["product_payments"]:
                        product_source_id = product["product_id"]
                        product_id = self._get_product_id(conn, product_source_id, order_ts)
                        if not product_id:
                            log.error("Продукт %s не найден в dm_products.", product_source_id)
                            continue

                        sale = ProductSaleDdsObj(
                            product_id=product_id,
                            order_id=order_id,
                            count=product["quantity"],
                            price=product["price"],
                            total_sum=product["product_cost"],
                            bonus_payment=product.get("bonus_payment", 0),
                            bonus_grant=product.get("bonus_grant", 0)
                        )
                        self.dds.insert_sale(conn, sale)

                except Exception as e:
                    log.error("Ошибка обработки транзакции %s: %s", tx.event_ts, str(e))