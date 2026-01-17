from typing import Any

from airflow.sdk import BaseOperator, Context
from airflow.exceptions import AirflowException
from dbt.cli.main import dbtRunner, dbtRunnerResult
import os

class DbtOperator(BaseOperator):
    def __init__(
        self,
        dbt_root_dir: str,
        dbt_command: str,
        target: str = None,
        select: str = None,
        dbt_vars: dict = None,
        full_refresh: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dbt_root_dir = dbt_root_dir
        self.dbt_command = dbt_command
        self.target = target
        self.select = select
        self.dbt_vars = dbt_vars
        self.full_refresh = full_refresh
        self.runner = dbtRunner()

    def execute(self, context: Context) -> Any:
        #VALIDATE dbt_root_dir exists
        if not os.path.exists(self.dbt_root_dir):
            raise AirflowException(f"dbt_root_dir {self.dbt_root_dir} does not exist")
        
        logs_dir = os.path.join(self.dbt_root_dir, 'logs')
        if not os.path.exists(logs_dir):
            try:
                os.makedirs(logs_dir, mode=0o777)
                self.log.info(f"Created logs directory {logs_dir}")
            except Exception as e:
                self.log.error(f"Failed to create logs directory {logs_dir}: {str(e)}")
                raise AirflowException(f"Failed to create logs directory {logs_dir}: {str(e)}")
        
        #ENSURE THE DIRECTORY IS WRITABLE
        if not os.access(logs_dir, os.W_OK):
            try:
                os.chmod(logs_dir, 0o777)
                self.log.info(f"Changed permissions of logs directory {logs_dir}")
            except Exception as e:
                self.log.error(f"Failed to change permissions of logs directory {logs_dir}: {str(e)}")
                raise AirflowException(f"Failed to change permissions of logs directory {logs_dir}: {str(e)}")

        
        #SPLIT THE dbt_command IF IT CONTAINS MULTIPLE ARGUMENTS
        if isinstance(self.dbt_command, str):
            command_parts = self.dbt_command.split()
        else:
            command_parts = [self.dbt_command]
        
        command_args = command_parts + [
            "--project-dir", self.dbt_root_dir,
            "--profiles-dir", self.dbt_root_dir,
        ]

        if self.target:
            command_args += ["--target", self.target]

        if self.select:
            command_args += ["--select", self.select]

        if self.full_refresh:
            command_args += ["--full-refresh"]

        if self.dbt_vars:
            vars_strings = ' '.join([f"{key}={value}" for key, value in self.dbt_vars.items()])
            command_args.extend(["--vars", vars_strings])

        self.log.info(f"Running dbt command: %s", ' '.join(command_args))
        

        res: dbtRunnerResult = self.runner.invoke(command_args)

        if res.success:
            self.log.info("dbt command executed successfully")
            if res.result:
                try:
                    for r in res.result:
                        if hasattr(r, 'error') and hasattr(r, 'status'):
                            self.log.info(f"{r.node.name}: {r.status}")
                except TypeError:
                    self.log.info("Command completed with result type: {type(res.result).__name__}")
            else:
                self.log.info("No results returned")
        else:
            self.log.error('dbt command failed')
            if res.exception:
                self.log.error(f"Exception: {res.exception}")
            raise AirflowException(f"dbt command failed: {' '.join(command_args)}")

        
        
        
