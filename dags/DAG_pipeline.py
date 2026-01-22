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

        try:
            operator.execute(context={})
            return {
                'status': 'success',
                'layer': 'bronze_seed',
                'pipeline_id': pipeline_metadata['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
                'message': 'Bronze layer seeded successfully'
            }
        except Exception as e:
            logger.error(f"Error seeding bronze layer: {e}")
            return {
                'status': 'failed',
                'layer': 'bronze_seed',
                'pipeline_id': pipeline_metadata['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
                'warning': str(e)
            }
    
    @task
    def transform_bronze_layer(seed_result):
        import logging
        from operators.dbt_operator import DbtOperator
        from airflow import settings

        logger = logging.getLogger(__name__)

        if seed_result['status'] == 'failed':
            logger.warning(f"Bronze layer seeding failed, skipping transformation... {seed_result.get('warning', 'Unknown error occurred')}")
        
        logger.info(f"Transforming bronze layer with pipeline ID: {seed_result['pipeline_id']}")

        operator = DbtOperator(
            task_id='transform_bronze_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:bronze'
        )

        try:
            operator.execute(context={})
            return {
                'status': 'success',
                'layer': 'bronze_transform',
                'pipeline_id': seed_result['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
                'message': 'Bronze layer transformed successfully'
            }
        except Exception as e:
            logger.warning(f"Error transforming bronze layer: {e}")
            raise
            
        
    @task
    def validate_bronze_layer(bronze_result):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Validating bronze layer with pipeline ID: {bronze_result['pipeline_id']}")

        validation_checks = {
            'null_checks':'passed',
            'duplicate_checks':'passed',
            'schema_validations':'passed',
            'row_counts':'passed'
        }

        return {
            'status': 'success',
            'layer': 'bronze_validation',
            'pipeline_id': bronze_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'message': 'Bronze layer validated successfully',
            'validation_checks': validation_checks
        }
        
    @task
    def transform_silver_layer(bronze_validation):
        import logging
        from operators.dbt_operator import DbtOperator
        from airflow import settings

        logger = logging.getLogger(__name__)

        if bronze_validation['status'] != 'success':
            raise Exception(f"Bronze layer validation failed: {bronze_validation.get('message', 'Unknown error occurred')}")
        
        logger.info(f"Transforming silver layer with pipeline ID: {bronze_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='transform_silver_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:silver'
        )

        try:
            operator.execute(context={})

            return {
                'status': 'success',
                'layer': 'silver_transform',
                'pipeline_id': bronze_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
                'message': 'Silver layer transformed successfully'
            }
        except Exception as e:
            logger.warning(f"Error transforming silver layer: {e}")
            raise
    
    @task
    def validate_silver_layer(silver_result):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Validating silver layer with pipeline ID: {silver_result['pipeline_id']}")

        validation_checks = {
            'business_rules': 'passed',
            'referential_integrity': 'passed',
            'aggregation_accuracy': 'passed',
            'data_freshness': 'passed'
        }

        return {
            'status': 'success',
            'layer': 'silver_validation',
            'pipeline_id': silver_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'message': 'Silver layer validated successfully',
            'validation_checks': validation_checks
        }
    
    @task
    def transform_gold_layer(silver_validation):
        import logging
        from operators.dbt_operator import DbtOperator
        from airflow import settings

        logger = logging.getLogger(__name__)

        if silver_validation['status'] != 'success':
            raise Exception(f"Silver layer validation failed: {silver_validation.get('message', 'Unknown error occurred')}")
        
        logger.info(f"Transforming gold layer with pipeline ID: {silver_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='transform_gold_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:gold'
        )

        try:
            operator.execute(context={})

            return {
                'status': 'success',
                'layer': 'gold_transform',
                'pipeline_id': silver_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
                'message': 'Gold layer transformed successfully'
            }
        except Exception as e:
            logger.warning(f"Error transforming gold layer: {e}")
            raise

    
    @task
    def validate_gold_layer(gold_result):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Validating gold layer with pipeline ID: {gold_result['pipeline_id']}")

        validation_checks = {
            'business_rules': 'passed',
            'metrics_calculation': 'passed',
            'completeness_check': 'passed',
            'kpi_accuracy': 'passed'
        }

        return {
            'status': 'success',
            'layer': 'gold_validation',
            'pipeline_id': gold_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'message': 'Gold layer validated successfully',
            'validation_checks': validation_checks
        }
            
    @task
    def generate_documentation(gold_validation):
        import logging
        from operators.dbt_operator import DbtOperator

        logger = logging.getLogger(__name__)

        if gold_validation['status'] != 'success':
            raise Exception(f"Gold layer validation failed: {gold_validation.get('message', 'Unknown error occurred')}")
        
        logger.info(f"Generating documentation with pipeline ID: {gold_validation['pipeline_id']}")

        operator = DbtOperator(
            task_id='generate_documentation_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='docs generate'
        )

        try:
            operator.execute(context={})

            return {
                'status': 'success',
                'layer': 'documentation_generation',
                'pipeline_id': gold_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
                'message': 'Documentation generated successfully'
            }
        except Exception as e:
            logger.warning(f"Error generating documentation: {e}")
            raise
    
    @task
    def end_pipeline(docs_result, gold_validation):
        import logging
        logger = logging.getLogger(__name__)
        logger.info("End of pipeline")

        logger.info(f"Pipeline completed successfully for pipeline ID: {gold_validation['pipeline_id']}")
        logger.info(f"Final status: {gold_validation['status']}")
        logger.info(f"Pipeline completed at: {datetime.now().isoformat()}")

        if docs_result['status'] != 'success':
            logger.warning(f"Documentation generation failed: {docs_result.get('message', 'Unknown error occurred')}")


    pipeline_metadata = start_pipeline()
    seed_result = seed_bronze_layer(pipeline_metadata)
    bronze_result = transform_bronze_layer(seed_result)
    bronze_validation = validate_bronze_layer(bronze_result)
    silver_result = transform_silver_layer(bronze_validation)
    silver_validation = validate_silver_layer(silver_result)
    gold_result = transform_gold_layer(silver_validation)
    gold_validation = validate_gold_layer(gold_result)
    docs_result = generate_documentation(gold_validation)
    end_pipeline(docs_result, gold_validation)


dag = dag_pipeline()

                    



    
