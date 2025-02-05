from collections import namedtuple
from datetime import datetime

from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from ..jobs.user_cumulated_job import do_user_cumulated_transformation

# Creating namedtuple for generating dataframe data
Events = namedtuple("Events", "user_id event_time")
UserCumulated = namedtuple("UserCumulated", "user_id date dates_active")


# A test to check when no data is present in the cumulative table
def test_no_previous_user_cumulated_data(spark: SparkSession):

    # Arange method
    events_data = [
        Events(user_id=1, event_time="2023-01-02 00:00:00"),
        Events(user_id=2, event_time="2023-01-02 00:00:00"),
        Events(user_id=3, event_time="2023-01-02 00:00:00"),
    ]

    events_df = spark.createDataFrame(events_data)

    # The key to the test, having an empty cumulative table
    user_cumulated_df = spark.createDataFrame(
        [], "user_id INT, date DATE, dates_active ARRAY<DATE>"
    )

    today_date = "2023-01-02"
    yesterday_date = "2022-01-01"

    # Act method
    actual_df = do_user_cumulated_transformation(
        spark=spark,
        user_cumulated_df=user_cumulated_df,
        events_df=events_df,
        yesterday_date=yesterday_date,
        today_date=today_date,
    )

    expected_date = datetime.strptime("2023-01-02", "%Y-%m-%d").date()

    expected_value = [
        UserCumulated(
            user_id=1,
            date=expected_date,
            dates_active=[expected_date],
        ),
        UserCumulated(
            user_id=2,
            date=expected_date,
            dates_active=[expected_date],
        ),
        UserCumulated(
            user_id=3,
            date=expected_date,
            dates_active=[expected_date],
        ),
    ]

    # Assert method
    expected_df = spark.createDataFrame(expected_value)
    assert_df_equality(actual_df, expected_df)


# A test to check when the todays fresh data contains duplicate rows
def test_when_duplicate_rows(spark: SparkSession):
    # Arange Method
    # The key to this test is in the events data when all the rows have same user id
    events_data = [
        Events(user_id=1, event_time="2023-01-02 00:00:00"),
        Events(user_id=1, event_time="2023-01-02 00:00:00"),
        Events(user_id=1, event_time="2023-01-02 00:00:00"),
    ]

    events_df = spark.createDataFrame(events_data)
    user_cumulated_df = spark.createDataFrame(
        [], "user_id INT, date DATE, dates_active ARRAY<DATE>"
    )
    today_date = "2023-01-02"
    yesterday_date = "2022-01-01"

    # Act method
    actual_df = do_user_cumulated_transformation(
        spark=spark,
        user_cumulated_df=user_cumulated_df,
        events_df=events_df,
        yesterday_date=yesterday_date,
        today_date=today_date,
    )

    expected_date = datetime.strptime("2023-01-02", "%Y-%m-%d").date()

    expected_value = [
        UserCumulated(
            user_id=1,
            date=expected_date,
            dates_active=[expected_date],
        ),
    ]

    # Assert Method
    expected_df = spark.createDataFrame(expected_value)
    assert_df_equality(actual_df, expected_df)
