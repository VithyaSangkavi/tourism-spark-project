"""
Sri Lanka Tourism Big Data Analytics and Recommendation System.

This script is designed for the Big Data Analytics mini project. It uses
PySpark for data loading, cleaning, analytics, feature engineering, and
recommendation generation.
"""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
from pyspark.ml.feature import HashingTF, IDF, StopWordsRemover, Tokenizer
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    countDistinct,
    desc,
    length,
    lit,
    lower,
    regexp_replace,
    round as spark_round,
    row_number,
    split,
    sum as spark_sum,
    trim,
    when,
)


PROJECT_DIR = Path(__file__).resolve().parent
DATA_PATH = PROJECT_DIR / "data" / "Destination Reviews_(raw).csv"
OUTPUT_DIR = PROJECT_DIR / "outputs"

VALID_DISTRICTS = [
    "Badulla",
    "Colombo",
    "Galle",
    "Gampaha",
    "Hambantota",
    "Hatton",
    "Kalmunai",
    "Kalutara",
    "Kurunagela",
    "Matale",
    "Matara",
    "Rathnapura",
]

POSITIVE_TERMS = [
    "amazing",
    "awesome",
    "beautiful",
    "best",
    "calm",
    "clean",
    "excellent",
    "fantastic",
    "good",
    "great",
    "love",
    "lovely",
    "peaceful",
    "perfect",
    "recommend",
    "scenic",
    "wonderful",
    "worth",
]

NEGATIVE_TERMS = [
    "bad",
    "boring",
    "crowded",
    "dirty",
    "disappointed",
    "expensive",
    "litter",
    "poor",
    "rude",
    "sad",
    "unsafe",
    "waste",
    "worst",
]

THEME_PATTERNS = {
    "Nature": r"\b(nature|green|forest|mountain|waterfall|lake|river|scenic|view|hike|trail|wildlife|bird|plain|ella)\b",
    "Beach": r"\b(beach|sea|ocean|sand|surf|coast|whale|boat|lagoon)\b",
    "Culture": r"\b(temple|kovil|church|fort|museum|heritage|history|historic|ancient|buddha|religious|palace)\b",
    "Family": r"\b(family|kids|children|park|picnic|play|garden|zoo|safari)\b",
    "Adventure": r"\b(hike|trek|climb|adventure|camp|cycling|surf|safari|trail|viewpoint)\b",
    "Relaxation": r"\b(calm|peaceful|relax|quiet|leisure|walk|beautiful|clean|evening)\b",
}

RECENCY_PATTERNS = {
    "Recent": r"\b(day|days|week|weeks|month|months)\s+ago\b",
    "Established": r"\b(year|years)\s+ago\b",
}


def start_spark() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder.appName("SriLankaTourismAnalyticsRecommendation")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )


def load_raw_data(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "ISO-8859-1")
        .csv(str(DATA_PATH))
    )


def clean_data(df: DataFrame) -> DataFrame:
    cleaned = (
        df.select(
            trim(col("Destination")).alias("Destination"),
            trim(col("District")).alias("District"),
            trim(col("Timespan")).alias("Timespan"),
            trim(col("Review")).alias("Review"),
        )
        .dropna(subset=["Destination", "District", "Review"])
        .filter(col("Destination") != "")
        .filter(col("District").isin(VALID_DISTRICTS))
        .filter(length(col("Review")) >= 10)
    )

    cleaned = cleaned.withColumn("review_lower", lower(col("Review")))
    cleaned = cleaned.withColumn(
        "review_clean",
        regexp_replace(col("review_lower"), r"[^a-z0-9\s]", " "),
    )
    cleaned = cleaned.withColumn(
        "review_clean",
        regexp_replace(col("review_clean"), r"\s+", " "),
    )
    return cleaned


def term_score(column: str, terms: list[str]):
    score = None
    for term in terms:
        term_hit = when(col(column).rlike(rf"\b{term}\b"), 1).otherwise(0)
        score = term_hit if score is None else score + term_hit
    return score


def add_sentiment(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("positive_score", term_score("review_clean", POSITIVE_TERMS))
        .withColumn("negative_score", term_score("review_clean", NEGATIVE_TERMS))
        .withColumn("sentiment_score", col("positive_score") - col("negative_score"))
        .withColumn(
            "sentiment",
            when(col("sentiment_score") > 0, "Positive")
            .when(col("sentiment_score") < 0, "Negative")
            .otherwise("Neutral"),
        )
    )


def add_experience_features(df: DataFrame) -> DataFrame:
    featured = df.withColumn("word_count", length(col("review_clean")) - length(regexp_replace(col("review_clean"), " ", "")) + 1)

    featured = featured.withColumn(
        "recency_segment",
        when(lower(col("Timespan")).rlike(RECENCY_PATTERNS["Recent"]), "Recent")
        .when(lower(col("Timespan")).rlike(RECENCY_PATTERNS["Established"]), "Established")
        .otherwise("Unknown"),
    )

    theme_expr = lit("General")
    for theme, pattern in reversed(THEME_PATTERNS.items()):
        theme_expr = when(col("review_clean").rlike(pattern), lit(theme)).otherwise(theme_expr)

    return featured.withColumn("travel_theme", theme_expr)


def print_dataset_summary(df: DataFrame) -> None:
    print("\n=== Dataset Summary ===")
    print(f"Clean review rows: {df.count():,}")
    print(f"Destinations: {df.select('Destination').distinct().count():,}")
    print(f"Districts: {df.select('District').distinct().count():,}")
    df.groupBy("District").count().orderBy(desc("count")).show(20, truncate=False)


def run_analytics(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    print("\n=== Part A: Spark Analytics ===")

    top_destinations = df.groupBy("Destination", "District").count().orderBy(desc("count"))
    print("\nTop destinations by review volume:")
    top_destinations.show(10, truncate=False)

    sentiment_summary = (
        df.groupBy("sentiment")
        .count()
        .withColumn("percentage", spark_round(col("count") / df.count() * 100, 2))
        .orderBy(desc("count"))
    )
    print("\nOverall sentiment distribution:")
    sentiment_summary.show(truncate=False)

    global_positive_rate = df.agg(
        avg(when(col("sentiment") == "Positive", 1.0).otherwise(0.0))
    ).first()[0]
    prior_strength = 50

    destination_quality = (
        df.groupBy("Destination", "District")
        .agg(
            count("*").alias("review_count"),
            spark_sum(when(col("sentiment") == "Positive", 1).otherwise(0)).alias(
                "positive_reviews"
            ),
            spark_sum(when(col("sentiment") == "Negative", 1).otherwise(0)).alias(
                "negative_reviews"
            ),
            avg("sentiment_score").alias("avg_sentiment_score"),
            avg("word_count").alias("avg_review_words"),
            countDistinct("travel_theme").alias("theme_diversity"),
        )
        .withColumn(
            "positive_rate",
            spark_round(col("positive_reviews") / col("review_count"), 3),
        )
        .withColumn(
            "bayesian_positive_rate",
            spark_round(
                (col("positive_reviews") + lit(global_positive_rate * prior_strength))
                / (col("review_count") + lit(prior_strength)),
                3,
            ),
        )
        .withColumn(
            "popularity_score",
            spark_round(col("review_count") / (col("review_count") + lit(120)), 3),
        )
        .withColumn(
            "engagement_score",
            spark_round(
                when(col("avg_review_words") >= 55, 1.0).otherwise(col("avg_review_words") / 55),
                3,
            ),
        )
        .withColumn(
            "recommendation_score",
            spark_round(
                (col("bayesian_positive_rate") * 0.48)
                + (col("popularity_score") * 0.27)
                + (col("engagement_score") * 0.15)
                + (when(col("theme_diversity") >= 3, 1.0).otherwise(col("theme_diversity") / 3) * 0.10),
                3,
            ),
        )
        .orderBy(desc("recommendation_score"), desc("review_count"))
    )

    print("\nBest destinations using sentiment + popularity score:")
    destination_quality.show(10, truncate=False)

    district_window = Window.partitionBy("District").orderBy(
        desc("recommendation_score"), desc("review_count")
    )
    best_by_district = (
        destination_quality.withColumn("rank", row_number().over(district_window))
        .filter(col("rank") <= 3)
        .orderBy("District", "rank")
    )
    print("\nTop 3 recommended destinations per district:")
    best_by_district.show(40, truncate=False)

    district_insights = (
        df.groupBy("District")
        .agg(
            count("*").alias("review_count"),
            countDistinct("Destination").alias("destination_count"),
            spark_sum(when(col("sentiment") == "Positive", 1).otherwise(0)).alias(
                "positive_reviews"
            ),
            spark_sum(when(col("sentiment") == "Negative", 1).otherwise(0)).alias(
                "negative_reviews"
            ),
            avg("sentiment_score").alias("avg_sentiment_score"),
        )
        .withColumn("positive_rate", spark_round(col("positive_reviews") / col("review_count"), 3))
        .withColumn("reviews_per_destination", spark_round(col("review_count") / col("destination_count"), 1))
        .withColumn(
            "opportunity_score",
            spark_round(
                (col("positive_rate") * 0.55)
                + ((lit(1) - (col("reviews_per_destination") / (col("reviews_per_destination") + lit(160)))) * 0.45),
                3,
            ),
        )
        .orderBy(desc("review_count"))
    )
    print("\nDistrict-level demand and opportunity insights:")
    district_insights.show(20, truncate=False)

    category_hotspots = (
        df.groupBy("travel_theme", "Destination", "District")
        .agg(
            count("*").alias("theme_reviews"),
            spark_sum(when(col("sentiment") == "Positive", 1).otherwise(0)).alias(
                "theme_positive_reviews"
            ),
            avg(when(col("sentiment") == "Positive", 1.0).otherwise(0.0)).alias("theme_positive_rate"),
        )
        .withColumn(
            "theme_bayesian_positive_rate",
            (col("theme_positive_reviews") + lit(global_positive_rate * 25))
            / (col("theme_reviews") + lit(25)),
        )
        .withColumn(
            "theme_score",
            spark_round(
                (col("theme_bayesian_positive_rate") * 0.7)
                + (col("theme_reviews") / (col("theme_reviews") + lit(80)) * 0.3),
                3,
            ),
        )
    )
    theme_window = Window.partitionBy("travel_theme").orderBy(desc("theme_score"), desc("theme_reviews"))
    category_hotspots = (
        category_hotspots.withColumn("rank", row_number().over(theme_window))
        .filter(col("rank") <= 5)
        .orderBy("travel_theme", "rank")
    )
    print("\nTheme-based tourism hotspots:")
    category_hotspots.show(50, truncate=False)

    return destination_quality, best_by_district, district_insights, category_hotspots


def build_destination_profiles(df: DataFrame) -> DataFrame:
    profiles = df.groupBy("Destination", "District").agg(
        concat_ws(" ", collect_reviews_udf("Review")).alias("all_reviews"),
        count("*").alias("review_count"),
        avg("sentiment_score").alias("avg_sentiment_score"),
        avg(when(col("sentiment") == "Positive", 1.0).otherwise(0.0)).alias("positive_rate"),
    )

    tokenizer = Tokenizer(inputCol="all_reviews", outputCol="tokens")
    tokenized = tokenizer.transform(profiles)

    remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")
    filtered = remover.transform(tokenized)

    hashing_tf = HashingTF(
        inputCol="filtered_tokens", outputCol="raw_features", numFeatures=2048
    )
    featurized = hashing_tf.transform(filtered)

    idf = IDF(inputCol="raw_features", outputCol="features")
    return idf.fit(featurized).transform(featurized)


def cosine_similarity(a, b) -> float:
    if a is None or b is None:
        return 0.0
    norm_product = math.sqrt(float(a.dot(a))) * math.sqrt(float(b.dot(b)))
    if norm_product == 0:
        return 0.0
    return float(a.dot(b)) / norm_product


class RecommendationResult:
    """Small local result table with a Spark-like show() method for notebooks."""

    def __init__(self, rows: Iterable[tuple[str, str, int, float, float, float]]):
        self.rows = list(rows)
        self.columns = [
            "Destination",
            "District",
            "review_count",
            "avg_sentiment_score",
            "similarity",
            "hybrid_score",
        ]

    def show(self, n: int = 20, truncate: bool | int = True, vertical: bool = False) -> None:
        rows = self.rows[:n]
        if vertical:
            for index, row in enumerate(rows):
                print(f"-RECORD {index}-")
                for column, value in zip(self.columns, row):
                    print(f"{column}: {value}")
            return

        formatted_rows = [[str(value) for value in row] for row in rows]
        widths = [
            max(len(column), *(len(row[index]) for row in formatted_rows))
            if formatted_rows
            else len(column)
            for index, column in enumerate(self.columns)
        ]

        def border() -> str:
            return "+" + "+".join("-" * (width + 2) for width in widths) + "+"

        def line(values: list[str]) -> str:
            return (
                "|"
                + "|".join(
                    f" {value:<{widths[index]}} "
                    for index, value in enumerate(values)
                )
                + "|"
            )

        print(border())
        print(line(self.columns))
        print(border())
        for row in formatted_rows:
            print(line(row))
        print(border())

    def toPandas(self):
        import pandas as pd

        return pd.DataFrame(self.rows, columns=self.columns)


def recommend_similar_destinations(
    profiles: DataFrame, liked_destination: str, top_n: int = 5
) -> RecommendationResult:
    profile_rows = profiles.select(
        "Destination", "District", "review_count", "avg_sentiment_score", "features"
    ).collect()

    liked_row = next(
        (
            row
            for row in profile_rows
            if row["Destination"].lower() == liked_destination.lower()
        ),
        None,
    )
    if liked_row is None:
        raise ValueError(f"Destination not found: {liked_destination}")

    recommendations = []
    for row in profile_rows:
        if row["Destination"].lower() == liked_destination.lower():
            continue
        recommendations.append(
            (
                row["Destination"],
                row["District"],
                int(row["review_count"]),
                round(float(row["avg_sentiment_score"]), 3),
                round(cosine_similarity(row["features"], liked_row["features"]), 3),
                0.0,
            )
        )

    scored_recommendations = []
    for row in recommendations:
        popularity = row[2] / (row[2] + 120)
        positive_signal = max(min((row[3] + 2) / 4, 1), 0)
        hybrid_score = (row[4] * 0.6) + (positive_signal * 0.25) + (popularity * 0.15)
        scored_recommendations.append((*row[:5], round(hybrid_score, 3)))

    recommendations = sorted(
        scored_recommendations, key=lambda item: (item[5], item[4], item[2]), reverse=True
    )[:top_n]
    return RecommendationResult(recommendations)


def recommend_by_district(destination_quality: DataFrame, district: str, top_n: int = 5) -> DataFrame:
    return (
        destination_quality.filter(lower(col("District")) == district.lower())
        .select(
            "Destination",
            "District",
            "review_count",
            "positive_reviews",
            "negative_reviews",
            "positive_rate",
            "recommendation_score",
        )
        .orderBy(desc("recommendation_score"), desc("review_count"))
        .limit(top_n)
    )


def collect_reviews_udf(column_name: str):
    from pyspark.sql.functions import collect_list

    return collect_list(col(column_name))


def save_visualizations(destination_quality: DataFrame, sentiment_df: DataFrame) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    top_pd = destination_quality.orderBy(desc("review_count")).limit(10).toPandas()
    plt.figure(figsize=(12, 6))
    plt.bar(top_pd["Destination"], top_pd["review_count"], color="#2f6f73")
    plt.xticks(rotation=75, ha="right")
    plt.ylabel("Review count")
    plt.title("Top 10 Sri Lankan Tourism Destinations by Review Volume")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "top_destinations_latest.png", dpi=160)
    plt.close()

    sentiment_pd = sentiment_df.toPandas()
    plt.figure(figsize=(7, 5))
    plt.bar(sentiment_pd["sentiment"], sentiment_pd["count"], color="#b65f41")
    plt.ylabel("Review count")
    plt.title("Overall Review Sentiment Distribution")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "sentiment_distribution_latest.png", dpi=160)
    plt.close()

    district_pd = destination_quality.groupBy("District").agg(
        avg("recommendation_score").alias("avg_score")
    ).orderBy(desc("avg_score")).toPandas()
    plt.figure(figsize=(11, 6))
    plt.bar(district_pd["District"], district_pd["avg_score"], color="#456990")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Average destination score")
    plt.title("District Tourism Recommendation Strength")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "district_strength.png", dpi=160)
    plt.close()


def export_dashboard_data(
    destination_quality: DataFrame,
    best_by_district: DataFrame,
    district_insights: DataFrame,
    category_hotspots: DataFrame,
    profiles: DataFrame,
) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    similar = recommend_similar_destinations(profiles, "Riverston", top_n=7).toPandas()
    payload = {
        "top_destinations": destination_quality.orderBy(desc("review_count"))
        .limit(10)
        .toPandas()
        .to_dict("records"),
        "best_by_district": best_by_district.toPandas().to_dict("records"),
        "district_insights": district_insights.toPandas().to_dict("records"),
        "category_hotspots": category_hotspots.toPandas().to_dict("records"),
        "similar_to_riverston": similar.to_dict("records"),
    }
    with open(OUTPUT_DIR / "dashboard_data.json", "w", encoding="utf-8") as file:
        import json

        json.dump(payload, file, indent=2)


def main() -> None:
    spark = start_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = load_raw_data(spark)
    print("\nRaw schema:")
    raw_df.printSchema()

    df = add_experience_features(add_sentiment(clean_data(raw_df))).cache()
    print_dataset_summary(df)

    destination_quality, best_by_district, district_insights, category_hotspots = run_analytics(df)

    sentiment_df = (
        df.groupBy("sentiment")
        .count()
        .withColumn("percentage", spark_round(col("count") / df.count() * 100, 2))
        .orderBy(desc("count"))
    )

    print("\n=== Part B: Recommendation System ===")
    print("\nDistrict-based recommendations for Colombo:")
    recommend_by_district(destination_quality, "Colombo").show(truncate=False)

    print("\nContent-based recommendations similar to Riverston:")
    profiles = build_destination_profiles(df).cache()
    recommend_similar_destinations(profiles, "Riverston").show(truncate=False)

    print("\nEvaluation-style checks:")
    print(f"Recommendation coverage: {profiles.select('Destination').distinct().count()} destinations")
    print("The recommender ranks destinations using review text similarity and sentiment quality.")

    save_visualizations(destination_quality, sentiment_df)
    export_dashboard_data(
        destination_quality,
        best_by_district,
        district_insights,
        category_hotspots,
        profiles,
    )
    print(f"\nVisualizations saved in: {OUTPUT_DIR}")
    print(f"Dashboard JSON saved in: {OUTPUT_DIR / 'dashboard_data.json'}")

    spark.stop()


if __name__ == "__main__":
    main()
