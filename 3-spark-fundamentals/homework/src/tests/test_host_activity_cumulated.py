from collections import namedtuple
from datetime import datetime

from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from ..jobs.host_activity_cumulated_job import do_host_activity_cumulated_transformation

# Creating namedtuple for generating dataframe data
Events = namedtuple("Events", "host event_time")
HostsCumulated = namedtuple("HostsCumulated", "host date host_activity_datelist")


# A test to check when no data is present in the cumulative table
def test_no_previous_host_activity_cumulated_data(spark: SparkSession):

    # Arange method
    todays_data = [
        Events(host=10, event_time="2023-01-20 00:00:00"),
        Events(host=20, event_time="2023-01-20 00:00:00"),
        Events(host=30, event_time="2023-01-20 00:00:00"),
        Events(host=40, event_time="2023-01-20 00:00:00"),
        Events(host=50, event_time="2023-01-20 00:00:00"),
    ]

    todays_df = spark.createDataFrame(todays_data)

    # The key to the test, having an empty cumulative table of yesterday date
    yesterday_df = spark.createDataFrame(
        [], "host INT, date DATE, host_activity_datelist ARRAY<DATE>"
    )

    # Act method
    actual_df = do_host_activity_cumulated_transformation(
        spark=spark,
        yesterday_df=yesterday_df,
        todays_df=todays_df,
    )

    expected_date = datetime.strptime("2023-01-20", "%Y-%m-%d").date()

    expected_value = [
        HostsCumulated(
            host=10,
            date=expected_date,
            host_activity_datelist=[expected_date],
        ),
        HostsCumulated(
            host=20,
            date=expected_date,
            host_activity_datelist=[expected_date],
        ),
        HostsCumulated(
            host=30,
            date=expected_date,
            host_activity_datelist=[expected_date],
        ),
        HostsCumulated(
            host=40,
            date=expected_date,
            host_activity_datelist=[expected_date],
        ),
        HostsCumulated(
            host=50,
            date=expected_date,
            host_activity_datelist=[expected_date],
        ),
    ]

    # Assert method
    expected_df = spark.createDataFrame(expected_value)
    assert_df_equality(actual_df, expected_df)


# A test to check when the todays data contains duplicate rows and
# whether it will merge into one row in the cumulative table
def test_duplicate_rows_in_todays_data(spark: SparkSession):
    # Arange Method
    # The key to this test is in the todays data when all the rows have same host
    todays_data = [
        Events(host=101, event_time="2023-01-20 00:00:00"),
        Events(host=101, event_time="2023-01-20 00:00:00"),
        Events(host=101, event_time="2023-01-20 00:00:00"),
        Events(host=101, event_time="2023-01-20 00:00:00"),
        Events(host=101, event_time="2023-01-20 00:00:00"),
    ]

    todays_df = spark.createDataFrame(todays_data)
    yesterday_df = spark.createDataFrame(
        [], "host INT, date DATE, host_activity_datelist ARRAY<DATE>"
    )

    # Act method
    actual_df = do_host_activity_cumulated_transformation(
        spark=spark,
        yesterday_df=yesterday_df,
        todays_df=todays_df,
    )

    expected_date = datetime.strptime("2023-01-20", "%Y-%m-%d").date()

    expected_value = [
        HostsCumulated(
            host=101,
            date=expected_date,
            host_activity_datelist=[expected_date],
        ),
    ]

    # Assert method
    expected_df = spark.createDataFrame(expected_value)
    assert_df_equality(actual_df, expected_df)
