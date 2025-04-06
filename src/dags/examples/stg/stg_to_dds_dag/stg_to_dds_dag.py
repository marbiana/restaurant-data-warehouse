import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from lib import ConnectionBuilder
from lib import PgConnect

from examples.stg.stg_to_dds_dag.dds_settings_repository import DdsEtlSettingsRepository
#from examples.stg.stg_to_dds_dag.stg.fct_products_loader import FctProductsLoader
from examples.stg.stg_to_dds_dag.order_loader import OrderLoader
from examples.stg.stg_to_dds_dag.products_loader import ProductLoader
from examples.stg.stg_to_dds_dag.restaurant_loader import RestaurantLoader
#from examples.stg.stg_to_dds_dag.stg.schema_ddl import SchemaDdl
from examples.stg.stg_to_dds_dag.timestamp_loader import TimestampLoader
from examples.stg.stg_to_dds_dag.user_loader import UserLoader
from examples.stg.stg_to_dds_dag.sales_loader import SalesLoader

log = logging.getLogger(__name__)

with DAG(
    dag_id='sprint5_stg_to_dds_snowflake_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'raw', 'dds'],
    is_paused_upon_creation=False
) as dag:
    
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    settings_repository = DdsEtlSettingsRepository()
    
    #@task(task_id="schema_init")
    #def schema_init(ds=None, **kwargs):
    #    rest_loader = SchemaDdl(dwh_pg_connect)
    #    rest_loader.init_schema()
    
    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants(ds=None, **kwargs):
        rest_loader = RestaurantLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_restaurants()
    
    @task(task_id="dm_products_load")
    def load_dm_products(ds=None, **kwargs):
        prod_loader = ProductLoader(dwh_pg_connect, settings_repository)
        prod_loader.load_products()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps(ds=None, **kwargs):
        ts_loader = TimestampLoader(dwh_pg_connect, settings_repository)
        ts_loader.load_timestamps()

    @task(task_id="dm_users_load")
    def load_dm_users(ds=None, **kwargs):
        user_loader = UserLoader(dwh_pg_connect, settings_repository)
        user_loader.load_users()
    
    @task(task_id="dm_orders_load")
    def load_dm_orders(ds=None, **kwargs):
        order_loader = OrderLoader(dwh_pg_connect, settings_repository)
        order_loader.load_orders()
    
    @task(task_id="fct_order_products_load")
    def load_fct_order_products(ds=None, **kwargs):
        fct_loader = SalesLoader(dwh_pg_connect, settings_repository)
        fct_loader.load_sales()

    #init_schema = schema_init()
#dm_restaurants = load_dm_restaurants()
#dm_products = load_dm_products()
#dm_timestamps = load_dm_timestamps()
#dm_users = load_dm_users()
#dm_orders = load_dm_orders()
#fct_order_products = load_fct_order_products()

    #init_schema >> dm_restaurants  # type: ignore
    #init_schema >> dm_timestamps  # type: ignore
    #init_schema >> dm_users  # type: ignore
    #init_schema >> dm_products  # type: ignore
    #init_schema >> dm_orders  # type: ignore
    
    #dm_restaurants >> dm_products  # type: ignore
    #dm_restaurants >> dm_orders  # type: ignore
    #dm_timestamps #>> dm_orders  # type: ignore
    #dm_users >> dm_orders  # type: ignore
    #dm_products >> fct_order_products  # type: ignore
    #dm_orders >> fct_order_products  # type: ignore
    dm_users_task = load_dm_users()
    dm_restaurants_task = load_dm_restaurants()
    dm_timestamps_task = load_dm_timestamps()
    dm_products_task = load_dm_products()
    dm_orders_task = load_dm_orders()
    fct_order_products_task = load_fct_order_products()
        
    (dm_users_task >> dm_restaurants_task >> dm_timestamps_task >> dm_products_task >> dm_orders_task >> fct_order_products_task)





