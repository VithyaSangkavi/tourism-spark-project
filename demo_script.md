# Demo Video Script

Project title: Sri Lanka Tourism Big Data Intelligence and Recommendation System

Estimated duration: 4 to 6 minutes

## 1. Introduction

Hello, my name is [your name]. This is my Big Data Analytics mini project titled
Sri Lanka Tourism Big Data Intelligence and Recommendation System.

The objective of this project is to analyze Sri Lankan tourism destination
reviews using Apache Spark and build a recommendation system that suggests
destinations using district preference, travel themes, sentiment quality, and
review-text similarity.

This project covers both parts of the assignment. Part A focuses on big data
analytics using PySpark, and Part B focuses on a tourism recommendation system.

## 2. Dataset Explanation

The dataset used in this project is `Destination Reviews_(raw).csv`.

It contains four main columns:

- Destination
- District
- Timespan
- Review

The reviews are text comments written by tourists about different destinations
in Sri Lanka. The dataset contains noisy text and some malformed rows, so data
cleaning is an important part of the project.

## 3. Environment and Spark Setup

Here I start the Spark session using PySpark.

The project is implemented in Python using:

- PySpark for big data processing
- Spark ML for TF-IDF feature extraction
- Matplotlib for visualizations
- Pandas only for small chart outputs

## 4. Data Loading

In this section, I load the CSV dataset using Spark.

I use options such as header support, multiline support, quote handling, escape
handling, and encoding support because review text can contain commas, quotes,
and multiple lines.

After loading the dataset, I display the first few rows and print the schema to
confirm that Spark has loaded the data correctly.

## 5. Data Cleaning and Preprocessing

Next, I clean the dataset.

The cleaning process includes:

- Removing rows with missing destination, district, or review values
- Keeping only valid tourism districts
- Removing very short reviews
- Converting reviews to lowercase
- Removing punctuation and extra spaces
- Creating review word-count and recency features
- Classifying reviews into tourism themes such as nature, beach, culture,
  family, adventure, and relaxation

This step is important because the raw dataset contains some malformed records.
After cleaning, the dataset contains more than 35,000 valid review records,
covering 236 destinations across 12 districts.

## 6. Sentiment Processing

For sentiment analysis, I use a simple lexicon-based method.

The system checks review text for positive words such as good, beautiful,
amazing, peaceful, and excellent. It also checks for negative words such as bad,
poor, dirty, unsafe, and worst.

Based on the positive and negative word counts, each review is classified as
Positive, Negative, or Neutral.

This sentiment score is later used for ranking destinations.

## 7. Part A: Big Data Analytics

Now I run the Spark analytics section.

First, the system shows the top destinations by review volume. This identifies
which destinations have the highest number of tourist reviews.

Next, it shows the review distribution by district. This helps identify which
districts have the most tourism activity in the dataset.

Then, the system displays the overall sentiment distribution. In this project,
most reviews are positive or neutral, which suggests that tourists generally
have favorable experiences.

The project also creates a stronger confidence-weighted destination quality
score. This score combines:

- Bayesian positive sentiment rate
- Popularity confidence based on review volume
- Review engagement using average review length
- Theme diversity

This is better than using only review count because a destination should be
popular, positively reviewed, and supported by enough evidence.

## 8. District-Level Insights

The system also shows the top three recommended destinations for each district
and calculates a district opportunity score.

This is useful because a tourist may already know the district they want to
visit, and the system can suggest the strongest destinations in that area.

For example, in Colombo, the system recommends Beddagana Wetland Park, Diyatha
Uyana, and Mount Lavania Beach.

## 9. Theme-Based Hotspots

The project also includes a theme-based analytics layer.

Spark scans the review text for tourism experience signals such as nature,
beach, culture, family, adventure, and relaxation.

For each theme, the system ranks destinations using theme review count and a
Bayesian positive-rate score. This prevents a destination with only one or two
positive reviews from unfairly ranking at the top.

## 10. Visualizations

The project generates visualizations and saves them in the `outputs` folder.

The first chart shows the top tourism destinations by review volume.

The second chart shows the overall sentiment distribution.

The third chart shows district-level recommendation strength.

These visualizations can be used in the presentation slides to explain the main
findings clearly.

## 11. Part B: Recommendation System

For Part B, I implemented three recommendation views.

The first approach is district-based recommendation. The user enters a district,
and the system recommends top destinations in that district based on sentiment
and confidence-weighted destination quality.

The second approach is theme-based recommendation. The user can select a travel
theme and see the strongest destinations for that type of experience.

The third approach is hybrid content-based recommendation. This uses review text
to find destinations that are similar to a selected destination, then blends text
similarity with sentiment and popularity confidence.

## 12. TF-IDF Hybrid Recommendation

For the content-based recommender, I group reviews by destination and create a
combined review profile for each destination.

Then I use Spark ML to apply:

- Tokenization
- Stop word removal
- HashingTF
- IDF

This creates TF-IDF feature vectors for every destination.

The system then calculates cosine similarity between the selected destination
and other destinations. Finally, it creates a hybrid score using similarity,
sentiment quality, and popularity confidence.

For example, when I select Riverston, the system recommends similar destinations
such as Pitawala Pathana, Hulangala Mini World's End View, Bambarakiri Ella,
Sembuwatta Lake, and Horton Plains National Park.

These recommendations make sense because they are mostly nature, mountain, and
scenic destinations.

## 13. Demo UI

The project includes a demo dashboard in `demo_ui.html`.

The dashboard shows the Spark dataset summary, demand analytics, district
recommendations, theme hotspots, hybrid Riverston recommendations, and generated
visualizations.

This makes the project easier to present because the output looks like a small
analytics product rather than only a console script.

## 14. Evaluation and Limitations

The recommendation system covers 236 destination profiles.

Since the dataset does not contain explicit user ratings, the project uses
implicit signals such as review text, sentiment, and review count.

One limitation is that the sentiment analysis is lexicon-based, so it may not
understand complex language, sarcasm, or mixed opinions.

Another limitation is that collaborative filtering cannot be fully applied
because there are no user IDs or rating values in the dataset.

## 15. Future Improvements

In the future, this project can be improved by:

- Using a trained machine learning model for sentiment analysis
- Adding user ratings and user IDs for collaborative filtering
- Adding maps, coordinates, and travel-time features
- Adding seasonality-based recommendations
- Deploying the dashboard as a full web application

## 16. Conclusion

In conclusion, this project demonstrates how Apache Spark can be used to process
and analyze tourism review data.

The project extracts useful insights about popular destinations, district-level
tourism demand, theme-based travel experiences, and tourist sentiment.

It also builds a recommendation system using district ranking, theme ranking,
and hybrid TF-IDF content similarity.

Thank you.
