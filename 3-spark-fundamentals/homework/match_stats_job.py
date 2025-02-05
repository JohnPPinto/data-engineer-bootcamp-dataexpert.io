"""
Your goal is to make the following things happen:

* Build a Spark job that
    1. Disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    2. Explicitly broadcast JOINs medals and maps
    3. Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
    4.Aggregate the joined data frame to figure out questions like:
        a. Which player averages the most kills per game?
        b. Which playlist gets played the most?
        c. Which map gets played the most?
        d. Which map do players get the most Killing Spree medals on?
    5. With the aggregated data set
        a. Try different .sortWithinPartitions to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast


def broadcast_join_maps(spark: SparkSession):
    # Getting the data and creating the Dataframes
    matches_df = spark.read.csv(
        "./3-spark-fundamentals/data/matches.csv", header=True, inferSchema=True
    )
    maps_df = spark.read.csv(
        "./3-spark-fundamentals/data/maps.csv", header=True, inferSchema=True
    )

    # Broadcast joining both the dataframe
    matches_maps_df = matches_df.join(other=broadcast(maps_df), on="mapid")

    return matches_df, maps_df, matches_maps_df


def broadcast_join_medals(spark: SparkSession):
    # Getting the data and creating the Dataframes
    medals_df = spark.read.csv(
        "./3-spark-fundamentals/data/medals.csv",
        header=True,
        inferSchema=True,
    )
    medals_matches_players_df = spark.read.csv(
        "./3-spark-fundamentals/data/medals_matches_players.csv",
        header=True,
        inferSchema=True,
    )

    # Broadcast joining both the dataframe
    medals_players_df = medals_matches_players_df.join(
        other=broadcast(medals_df),
        on="medal_id",
    )

    return medals_df, medals_matches_players_df, medals_players_df


def get_bucketed_dataframe(
    spark: SparkSession,
    bucket_table_name: str,
    dataframe: DataFrame,
    partition_col: str,
    sort_col,
):
    num_buckets = 16

    # Repartitioning and sorting within the partition
    spark.sql(sqlQuery=f"DROP TABLE IF EXISTS {bucket_table_name}")
    dataframe = dataframe.repartition(
        num_buckets,
        partition_col,
    ).sortWithinPartitions(sort_col)

    # Writing the dataframe into a file using the bucket method
    dataframe.write.format("parquet").bucketBy(
        num_buckets,
        partition_col,
    ).mode(
        "overwrite"
    ).saveAsTable(bucket_table_name)

    # Read bucketed table
    bucketed_df = spark.table(tableName=bucket_table_name)
    return bucketed_df


def get_aggregate_stats(df: DataFrame, medals_df: DataFrame):
    # 4.a. Get the average kills per game for each player
    df.groupBy("match_id", "player_gamertag").avg("player_total_kills").alias(
        "avg_kills_per_game"
    ).show()

    # 4.b. Get the most common playlist
    most_common_playlist = (
        df.groupBy("playlist_id").count().orderBy("count", ascending=False).take(1)
    )
    print(f"The most common playlist is: {most_common_playlist}")

    # 4.c. Get the most common map
    most_common_map = (
        df.groupBy("mapid").count().orderBy("count", ascending=False).take(1)
    )
    print(f"The most common map is: {most_common_map}")

    # 4.d. Get the most common map for Killing Spree medals
    df = df.join(broadcast(medals_df), "medal_id")
    killing_spree = (
        df.filter(df.classification == "KillingSpree")
        .groupBy("mapid")
        .count()
        .orderBy("count", ascending=False)
        .take(1)
    )
    print(f"The most common map for killing spree is: {killing_spree}")


def compare_file_size(df_name_list: list):
    # Comparing the size of the different bucketed dataframe
    for name in df_name_list:
        file_path = "./spark-warehouse/" + name
        file_size_list = []
        if os.path.exists(file_path):
            for file in os.listdir(file_path):
                file_size_list.append(os.path.getsize(file_path + "/" + file))
            print(f"Table: {name}, File Sizes: {sorted(file_size_list)}")


def main():

    # 1. Creates a SparkSession
    spark = (
        SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1)
        .appName("MatchesStats")
        .getOrCreate()
    )

    # 2. Broadcast join and displaying the dataframe
    matches_df, _, matches_maps_df = broadcast_join_maps(spark=spark)
    matches_maps_df.show()

    medals_df, medals_matches_players_df, medals_players_df = broadcast_join_medals(
        spark=spark
    )
    medals_players_df.show()

    # 3. Bucket join and displaying the dataframe
    # Creating a dataframe of match details
    match_details_df = spark.read.csv(
        "./3-spark-fundamentals/data/match_details.csv",
        header=True,
        inferSchema=True,
    )

    # Creating bucketed dataframe from the previous dataframe
    matches_bucketed = get_bucketed_dataframe(
        spark=spark,
        bucket_table_name="matches_bucketed",
        partition_col="match_id",
        dataframe=matches_df,
        sort_col="match_id",
    )
    match_details_bucketed = get_bucketed_dataframe(
        spark=spark,
        bucket_table_name="match_details_bucketed",
        partition_col="match_id",
        dataframe=match_details_df,
        sort_col="match_id",
    )
    medals_matches_players_bucketed = get_bucketed_dataframe(
        spark=spark,
        bucket_table_name="medals_matches_players_bucketed",
        partition_col="match_id",
        dataframe=medals_matches_players_df,
        sort_col="match_id",
    )

    # Joining all the dataframe which were bucketed
    bucketed_df = matches_bucketed.join(match_details_bucketed, "match_id").join(
        medals_matches_players_bucketed, ["match_id", "player_gamertag"]
    )
    bucketed_df.show()

    # 4. Getting the aggregated statistic from the bucketed dataframe
    get_aggregate_stats(df=bucketed_df, medals_df=medals_df)

    # 5. Getting different version of matches dataframe with sortWithinPartition
    # and aggregating and comparing their sizes

    # version 2:
    matches_bucketed_v2 = get_bucketed_dataframe(
        spark=spark,
        bucket_table_name="matches_bucketed_v2",
        partition_col="match_id",
        dataframe=matches_df,
        sort_col=["match_id", "playlist_id"],
    )

    bucketed_df_v2 = matches_bucketed_v2.join(match_details_bucketed, "match_id").join(
        medals_matches_players_bucketed, ["match_id", "player_gamertag"]
    )

    get_aggregate_stats(df=bucketed_df_v2, medals_df=medals_df)

    # version 3:
    matches_bucketed_v3 = get_bucketed_dataframe(
        spark=spark,
        bucket_table_name="matches_bucketed_v3",
        partition_col="match_id",
        dataframe=matches_df,
        sort_col=["match_id", "mapid", "playlist_id"],
    )

    bucketed_df_v3 = matches_bucketed_v3.join(match_details_bucketed, "match_id").join(
        medals_matches_players_bucketed, ["match_id", "player_gamertag"]
    )

    get_aggregate_stats(df=bucketed_df_v3, medals_df=medals_df)

    print("\nVersion 1:")
    compare_file_size(
        [
            "matches_bucketed",
            "match_details_bucketed",
            "medals_matches_players_bucketed",
        ]
    )
    print("\nVersion 2:")
    compare_file_size(
        [
            "matches_bucketed_v2",
            "match_details_bucketed",
            "medals_matches_players_bucketed",
        ]
    )
    print("\nVersion 3:")
    compare_file_size(
        [
            "matches_bucketed_v3",
            "match_details_bucketed",
            "medals_matches_players_bucketed",
        ]
    )


if __name__ == "__main__":
    main()
