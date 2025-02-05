from pyspark.sql import DataFrame, SparkSession


def do_user_cumulated_transformation(
    spark: SparkSession,
    user_cumulated_df: DataFrame,
    events_df: DataFrame,
    yesterday_date: str,
    today_date: str,
):
    # This query was in the fact data modeling lesson in week 2
    # Here the yesterday data and todays data is been cumulated for a data list array.
    query = f"""
	WITH
		yesterday AS (
			SELECT *
			FROM user_cumulated
			WHERE date = '{yesterday_date}'
		),
		today AS (
			SELECT
				user_id,
				DATE(CAST(event_time AS TIMESTAMP)) AS date_active
			FROM events
			WHERE
				DATE(CAST(event_time AS TIMESTAMP)) = '{today_date}'
				AND user_id IS NOT NULL
			GROUP BY user_id, date_active
		)
	SELECT
		COALESCE(t.user_id, y.user_id) AS user_id,
		COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date,
		CASE
			WHEN y.dates_active IS NULL
				THEN array(t.date_active)
			WHEN t.date_active IS NULL
				THEN y.dates_active
			ELSE concat(array(t.date_active), y.dates_active)
		END AS dates_active
	FROM today t
	FULL OUTER JOIN yesterday y
		ON t.user_id = y.user_id
    ORDER BY user_id;
	"""
    user_cumulated_df.createOrReplaceTempView("user_cumulated")
    events_df.createOrReplaceTempView("events")
    return spark.sql(sqlQuery=query)


def main():

    # Dates for getting the exact data
    yesterday_date = "2023-01-01"
    today_date = "2023-01-01"

    spark = (
        SparkSession.builder.master("local")
        .appName("user_cumulated_job")
        .getOrCreate(),
    )

    # Generating a cumulative dataframe based on the query
    output_df = do_user_cumulated_transformation(
        spark=spark,
        user_cumulated_df=spark.table("user_cumulated"),
        events_df=spark.table("events"),
        yesterday_date=yesterday_date,
        today_date=today_date,
    )
    output_df.write.mode("overwrite").insertInto("user_cumulated_table")
