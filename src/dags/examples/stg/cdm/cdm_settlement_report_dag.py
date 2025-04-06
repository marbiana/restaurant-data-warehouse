from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from lib import ConnectionBuilder
import logging

logger = logging.getLogger(__name__)

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    'cdm_settlement_report_dag',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Ежедневно в 01:00
    catchup=False,
    tags=['cdm', 'report']
) as dag:

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="update_settlement_report")
    def update_settlement_report():
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.dm_settlement_report (
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum
                    )
                    WITH closed_orders AS (
                        SELECT 
                            o.id AS order_id,
                            o.restaurant_id,
                            t.date AS settlement_date
                        FROM dds.dm_orders o
                        INNER JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                        WHERE o.order_status = 'CLOSED'
                    ),
                    order_details AS (
                        SELECT
                            co.restaurant_id,
                            co.settlement_date,
                            SUM(fps.total_sum) AS orders_total_sum,
                            SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
                            SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
                            COUNT(DISTINCT co.order_id) AS orders_count
                        FROM closed_orders co
                        INNER JOIN dds.fct_product_sales fps ON co.order_id = fps.order_id
                        GROUP BY co.restaurant_id, co.settlement_date
                    )
                    SELECT
                        r.id AS restaurant_id,
                        r.restaurant_name,
                        od.settlement_date,
                        od.orders_count,
                        od.orders_total_sum,
                        od.orders_bonus_payment_sum,
                        od.orders_bonus_granted_sum,
                        od.orders_total_sum * 0.25 AS order_processing_fee,
                        (od.orders_total_sum - od.orders_bonus_payment_sum - (od.orders_total_sum * 0.25)) AS restaurant_reward_sum
                    FROM order_details od
                    INNER JOIN dds.dm_restaurants r ON od.restaurant_id = r.id
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """)
                logger.info("Витрина dm_settlement_report успешно обновлена.")

    update_settlement_report = update_settlement_report()