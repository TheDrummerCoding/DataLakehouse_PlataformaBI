from datetime import timedelta, datetime

from airflow import settings
from airflow.decorators import dag, task

DBT_ROOT_DIR = f"{settings.DAGS_FOLDER}/ecommerce_dbt"
@dag(
    dag_id="ecommerce_dag_pipeline",
    default_args={
        "owner": "fabio",
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(seconds=15)
    },
    schedule=timedelta(hours=6), 
    start_date=datetime(2026, 1, 18),
    catchup=False,
    tags=['dbt', 'medallion', 'ecommerce'],
    max_active_runs=1
)

def dag_pipeline():

    @task
    def start_pipeline():
        import logging

        logger = logging.getLogger(__name__)
        logger.info("Starting the pipeline")

        pipeline_metadata = {
            'pipeline_start_time': datetime.now().isoformat(),
            'dbt_root_dir': DBT_ROOT_DIR,
            'pipeline_id': f'dag_pipeline_{datetime.now().strftime('%Y%m%d%H%M%S')}',
            'enviroment': 'production',
        }

        logger.info(f"STARTING PIPELINE WITH ID: {pipeline_metadata['pipeline_id']} ")
        return pipeline_metadata

    @task
    def seed_bronze_layer(pipeline_metadata):
        import logging
        from operators.dbt_operator import DbtOperator

        logger = logging.getLogger(__name__)
        logger.info("Seeding bronze layer...")

        try:
            import sqlalchemy
            from sqlalchemy import text

            engine = sqlalchemy.create_engine('trino://trino@trino-coordinator:8080/iceberg/bronze')

            with engine.connect() as connection:
                resultado = connection.execute(text("SELECT count(*) as cnt FROM raw_customer_events"))
                filas_totales = resultado.scalar()

                if filas_totales and filas_totales > 0:
                    logger.info(f"Seeding bronze layer with {filas_totales} rows, skepping seeding")
                    return {
                        'status': 'skipped',
                        'layer': 'bronze_seed',
                        'pipeline_id': pipeline_metadata['pipeline_id'],
                        'timestamp': datetime.now().isoformat(),
                        'message': f"Tables already seeded with {filas_totales} rows, skepping seeding"
                    }
        except Exception as e:
            logger.error(f"Tables dont exist or an error ocurred {e}")

        operator = DbtOperator(
            task_id='seed_bronze_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='seed',
            full_refresh=True,
        )

    
            
            
            
            

                    



    
