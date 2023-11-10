"""Publish a QJ result set to Tableau as a hyper extract"""
import argparse
from pathlib import Path
import sys
import tempfile
from typing import Any, Dict, List, Optional

from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    CreateMode,
    TableDefinition,
    TableName,
    SqlType,
    Inserter,
)
import tableauserverclient as TSC

from altimeter.core.log import Logger
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import PublishConfig
from altimeter.qj.log import QJLogEvents


def publish(event: Dict[str, Any]) -> None:
    """Publish a QJ result set to Tableau as a hyper extract"""
    config = PublishConfig()
    logger = Logger()
    logger.info(event=QJLogEvents.InitConfig, config=config)
    job_name = event.get("job_name")
    if not job_name:
        raise Exception("Missing expected input parameter 'job_name'")
    qj_client = QJAPIClient(host=config.api_host, port=config.api_port)
    logger.info(event=QJLogEvents.GetLatestResultSetStart, job_name=job_name)
    latest_result_set = qj_client.get_job_latest_result_set(job_name=job_name)
    if not latest_result_set:
        raise Exception(f"ERROR: no latest result set found for {job_name}")
    num_results = len(latest_result_set.results)
    logger.info(event=QJLogEvents.GetLatestResultSetEnd, job_name=job_name)

    logger.info(event=QJLogEvents.PublishResultSetToTableauStart, num_results=num_results)
    sorted_query_fields = sorted(latest_result_set.job.query_fields)
    columns: List[TableDefinition.Column] = [
        TableDefinition.Column(query_field, SqlType.text()) for query_field in sorted_query_fields
    ]
    columns.append(TableDefinition.Column("result_created", SqlType.timestamp()))
    with tempfile.TemporaryDirectory() as temp_dir_name:
        hyper_filepath = Path(
            temp_dir_name, f"AltiHyper-{config.env_name}-{job_name}.hyper"
        ).as_posix()
        with HyperProcess(
            Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
            parameters={"default_database_version": "2", "log_config": ""},
        ) as hyper:
            with Connection(
                hyper.endpoint, hyper_filepath, CreateMode.CREATE_AND_REPLACE
            ) as connection:
                table = TableDefinition(TableName(job_name), columns)
                connection.catalog.create_table(table)
                with Inserter(connection, table) as inserter:
                    for result in latest_result_set.results:
                        row = [
                            result.account_id
                            if query_field == "account_id"
                            else result.result[query_field]
                            for query_field in sorted_query_fields
                        ] + [
                            latest_result_set.created,
                        ]
                        inserter.add_row(row)
                    inserter.execute()
        tableau_auth = TSC.PersonalAccessTokenAuth(
            config.tableau_token_name,
            config.tableau_token_value.get_secret_value(),
            config.tableau_site_id,
        )
        if config.verify_ssl:
            server = TSC.Server(
                f"https://{config.tableau_server_hostname}", use_server_version=True
            )
        else:
            server = TSC.Server(
                f"https://{config.tableau_server_hostname}", use_server_version=False
            )
            server.version = "3.3"
            server.add_http_options({"verify": config.verify_ssl})
        publish_mode = TSC.Server.PublishMode.Overwrite
        with server.auth.sign_in(tableau_auth):
            get_projects_options = TSC.RequestOptions()
            get_projects_options.filter.add(
                TSC.Filter(
                    TSC.RequestOptions.Field.Name,
                    TSC.RequestOptions.Operator.Equals,
                    config.tableau_project_name,
                )
            )
            filtered_projects, _ = server.projects.get(req_options=get_projects_options)
            if not filtered_projects:
                raise ValueError(f"No project found with name {config.tableau_project_name}")
            if len(filtered_projects) > 1:
                raise ValueError(
                    f"More than one project found with name {config.tableau_project_name}"
                )
            datasource_project = filtered_projects.pop()
            datasource = TSC.DatasourceItem(datasource_project.id)
            datasource = server.datasources.publish(datasource, hyper_filepath, publish_mode)
            logger.info(
                event=QJLogEvents.PublishResultSetToTableauEnd,
                num_results=num_results,
                filepath=hyper_filepath,
                datasource_id=datasource.id,
                datasource_name=datasource.name,
            )


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("job_name", type=str)
    args_ns = parser.parse_args(argv)
    event = {"job_name": args_ns.job_name}
    publish(event=event)
    return 0


if __name__ == "__main__":
    sys.exit(main())
