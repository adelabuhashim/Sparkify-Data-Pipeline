from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow import conf
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    "owner": "adel_abu_hashim",
    "start_date": datetime.now(),
    "provide_context": True,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,  # When turned off, the scheduler creates a DAG run only for the latest interval.
    "email_on_retry": False,
    "dagrun_timeout": timedelta(minutes=60),
}


dag = DAG(
    "ETL_REDSHIFT",
    default_args=default_args,
    description="",
    schedule_interval="0 * * * *",
)
with open(os.path.join(conf.get("core", "dags_folder"), "create_tables.sql")) as f:
    create_tables_sql = f.read()

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql,
)


start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path="s3://udacity-dend/log_data",
    json_path="s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_path="s3://udacity-dend/song_data",
    json_path="auto",
)


load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    table="songplays",
)


load_song_dimension_table = LoadDimensionOperator(
    task_id="load_songs_dimension_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.song_table_insert,
    truncate=True,
    table="songs",
)


load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dimension_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.user_table_insert,
    table="users",
    truncate=True,
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artists_dimension_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    table="artists",
    truncate=True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dimension_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    table="time",
    truncate=True,
)


run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "songs", "artists", "time", "users"],
    schema="public",
    dq_checks=[
        {
            "check_sql": "select count(*) from {}.{} where title is null",
            "expected_result": 0,
        },
    ],
)


end_operator = DummyOperator(task_id="Stop_execution", dag=dag)
create_trips_table >> start_operator
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_song_dimension_table

run_quality_checks << load_user_dimension_table
run_quality_checks << load_artist_dimension_table
run_quality_checks << load_time_dimension_table
run_quality_checks << load_song_dimension_table

run_quality_checks >> end_operator
