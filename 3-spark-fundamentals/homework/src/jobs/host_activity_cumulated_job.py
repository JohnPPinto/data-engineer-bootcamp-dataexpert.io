from pyspark.sql import DataFrame, SparkSession


def do_host_activity_cumulated_transformation(
    spark: SparkSession,
    yesterday_df: DataFrame,
    todays_df: DataFrame,
):
    # This query was in the fact data modeling homework questions in week 2
    # Here the yesterday data and todays data is been
    # cumulated for a host activity list array.
    query = f"""
	WITH
		today AS (
			SELECT
				host,
				DATE(CAST(event_time AS TIMESTAMP)) AS date
			FROM events
			WHERE DATE(event_time) = DATE('2023-01-20')
			GROUP BY host, date
		),
		yesterday AS (
			SELECT *
			FROM hosts_cumulated
			WHERE date = DATE('2023-01-19')
		)
	SELECT
		COALESCE(t.host, y.host) AS host,
		CAST(COALESCE(t.date, y.date + INTERVAL '1 day') AS DATE) AS date,
		-- Cumulating all the dates by concatenating the yesterday data with todays data
		CASE
			WHEN y.host_activity_datelist IS NULL
				THEN array(t.date)
			WHEN t.date IS NULL
				THEN y.host_activity_datelist
			ELSE array(t.date) || y.host_activity_datelist
		END AS host_activity_datelist
	FROM today AS t
	FULL OUTER JOIN yesterday AS y
		ON t.host = y.host
    ORDER BY host;
	"""
    yesterday_df.createOrReplaceTempView("hosts_cumulated")
    todays_df.createOrReplaceTempView("events")
    return spark.sql(sqlQuery=query)


def main():

    spark = (
        SparkSession.builder.master("local").appName("host_activity").getOrCreate(),
    )

    # Generating a cumulative dataframe based on the query
    output_df = do_host_activity_cumulated_transformation(
        spark=spark,
        yesterday_df=spark.table("hosts_cumulated"),
        todays_df=spark.table("events"),
    )
    output_df.write.mode("overwrite").insertInto("host_activity_cumulated_table")
