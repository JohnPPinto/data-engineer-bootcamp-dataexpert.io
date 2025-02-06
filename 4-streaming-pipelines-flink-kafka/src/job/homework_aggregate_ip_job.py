# 1. Create a Flink job that sessionizes the input data by IP address and host
# 2. Use a 5 minute gap
# 3. Answer these questions
# 	1. What is the average number of web events of a session from a user on Tech Creator?
# 	2. Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

import logging
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables required:
# POSTGRES_URL: The URL of the Postgres database that stores the sink table
# POSTGRES_USER: The username to connect to the Postgres database that stores the sink data
# POSTGRES_PASSWORD: The password to connect to the Postgres database that stores the sink data
# KAFKA_URL: The URL of the Kafka broker to read the processed events
# KAFKA_TOPIC: The Kafka topic to read the processed events
# KAFKA_GROUP: The Kafka consumer group to read the processed events
# KAFKA_WEB_TRAFFIC_KEY: The Kafka key to authenticate with the Kafka broker


def create_aggregated_events_ip_sink_postgres(t_env):
    # Create a sink table in postgres to store the aggregated events
    table_name = "processed_events_ip_aggregated"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_window_timestamp TIMESTAMP(3),
            host VARCHAR,
            referrer VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env):
    # Create a source table from Kafka to read the processed events
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create source table from Kafka
        source_table = create_processed_events_source_kafka(t_env)

        # Create sink table for aggregated events in postgres
        aggregated_ip_sink_table = create_aggregated_events_ip_sink_postgres(t_env)

        # Create a table from the source table with a session window of 5 minutes and group by host and ip
        # Then insert the aggregated records into the sink table.
        t_env.from_path(source_table).window(
            Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")
        ).group_by(
            col("w"),
            col("host"),
            col("ip"),
        ).select(
            col("w").start.alias("event_window_timestamp"),
            col("host"),
            col("ip"),
            col("host").count.alias("num_hits"),
        ).execute_insert(
            aggregated_ip_sink_table
        ).wait()

    except Exception as e:
        logger.error("Writing records from Kafka to JDBC failed: %s", str(e))
        raise


if __name__ == "__main__":
    try:
        log_aggregation()
    except Exception as e:
        logger.error("Job failed: %s", str(e))
