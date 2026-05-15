# Presentation Speech Script

Project title: Sri Lanka Tourism Big Data Intelligence and Recommendation System

Estimated duration: 6 to 8 minutes

## Slide 1: Title

Good morning/afternoon. My project is titled **Sri Lanka Tourism Big Data
Intelligence and Recommendation System**.

This is a Big Data Analytics project built using Apache Spark and PySpark. The
main idea is to process tourism destination review data, extract useful
analytics, and build a recommendation system that can suggest destinations based
on district, travel theme, and review-text similarity.

I also created a demo dashboard so the final output is easier to understand and
present.

## Slide 2: Problem and Objective

The problem I focused on is that tourism review datasets contain a lot of useful
information, but the raw text is noisy and difficult to use directly.

So my objectives were:

- First, to analyze tourism demand by identifying popular destinations and
  active districts.
- Second, to measure destination quality using more than just review count.
- Third, to recommend destinations using district preference, travel theme, and
  similarity between review texts.

This makes the project both an analytics system and a recommendation system.

## Slide 3: Dataset

The dataset I used is `Destination Reviews_(raw).csv`.

It contains four main columns:

- Destination
- District
- Timespan
- Review

The raw dataset contains tourist review comments about different places in Sri
Lanka. Since review text can contain commas, quotation marks, multiline text,
and some encoding issues, I had to handle the data carefully when loading it.

After cleaning, the dataset contains **35,509 valid review records**, covering
**236 destination profiles** across **12 valid districts**.

I also created **7 travel themes**, such as nature, beach, culture, family,
adventure, relaxation, and general.

## Slide 4: Spark Processing

This slide shows the full Big Data pipeline.

First, I used Spark to load the CSV file. I used options such as header support,
multiline support, quote handling, escape handling, and encoding support because
review text is not always simple one-line text.

Second, I cleaned the dataset. I removed rows with missing destination, district,
or review values. I also filtered only valid districts, removed very short
reviews, converted text to lowercase, removed punctuation, and normalized extra
spaces.

Third, I performed feature engineering. I created sentiment features using a
lexicon-based method. I also created review word-count, recency segment, and
travel theme features.

Finally, I used Spark aggregations and window functions to generate destination
rankings, district insights, theme hotspots, and recommendation outputs.

## Slide 5: Part A Analytics

For Part A, I focused on Big Data analytics using Spark.

The first analysis is review volume by destination. This helps identify which
destinations are most discussed by tourists. For example, destinations like
Horton Plains National Park, Riverston, Moon Plains, and Bambarakiri Ella have
high review activity.

I also analyzed review activity by district. Districts such as Matale, Colombo,
Hatton, and Matara show strong tourism activity in this dataset.

Then I calculated sentiment distribution. Most reviews are positive or neutral,
while negative reviews are a small percentage.

The important point is that I did not only rank destinations by popularity. A
place can have many reviews but not necessarily strong quality. So I built a
more balanced scoring method.

## Slide 6: Scoring Model

This is one of the main improvements in my project.

Instead of using only review count or only sentiment, I created a
confidence-weighted recommendation score.

The final score uses four signals:

- **Bayesian positive rate**, weighted at 48 percent
- **Popularity confidence**, weighted at 27 percent
- **Review engagement**, weighted at 15 percent
- **Theme diversity**, weighted at 10 percent

The Bayesian positive rate is useful because it reduces the risk of over-ranking
a destination that has only a few positive reviews.

Popularity confidence gives value to destinations with more evidence from many
reviews.

Review engagement uses average review length, because longer reviews usually
contain more useful experience information.

Theme diversity rewards destinations that appear across different tourism
experience categories.

Using this score, the top results include destinations such as Raja & the Whales,
Horton Plains National Park, Beddagana Wetland Park, and Mirissa Beach.

## Slide 7: District Recommender

For the district-based recommender, the user selects a district, and the system
recommends the strongest destinations in that district.

For example, in Colombo, the top recommendations are:

- Beddagana Wetland Park
- Diyatha Uyana
- Mount Lavania Beach

These are ranked using the confidence-weighted destination score.

I also calculated district-level insights, such as total review demand,
destination coverage, positive sentiment rate, reviews per destination, and
opportunity score.

This is useful because it can show not only which districts are popular, but also
which districts may have growth potential.

## Slide 8: Theme Hotspots

The next part is theme-based recommendation.

I created travel themes by scanning the cleaned review text for experience
signals. For example:

- Beach-related words are used for beach destinations.
- Nature-related words are used for natural attractions.
- Temple, fort, museum, and historical words are used for culture.
- Family, kids, park, and safari words are used for family experiences.

Then I grouped reviews by travel theme and destination.

To make the ranking fair, I used Bayesian smoothing again. This prevents a
destination with only one or two positive theme reviews from ranking too highly.

Examples from the system include Mirissa Beach for beach travel, Japanese Peace
Pagoda for nature, and Sembuwatta Lake or Riverston for relaxation.

This makes the recommendation system more flexible because different tourists
may prefer different experience types.

## Slide 9: Hybrid TF-IDF Recommendation System

For Part B, I implemented a content-based recommendation system using Spark ML.

First, I grouped all reviews for each destination into one combined destination
profile.

Then I used Spark ML feature extraction:

- Tokenizer to split review text into words
- StopWordsRemover to remove common words
- HashingTF to convert words into term-frequency vectors
- IDF to reduce the weight of common words and highlight important words

This creates a TF-IDF feature vector for every destination.

Then I calculated cosine similarity between the selected destination and every
other destination.

For example, when the selected destination is Riverston, the system recommends
similar destinations such as Pitawala Pathana, Horton Plains National Park,
Sembuwatta Lake, and Bambarakiri Ella.

I also made it hybrid by blending text similarity with sentiment quality and
popularity confidence. This means the recommender does not only return textually
similar places, but also considers whether those places are positively reviewed
and supported by enough review data.

## Slide 10: Evaluation and Conclusion

The recommendation system covers **236 destination profiles**, which means every
clean destination profile can participate in the recommendation process.

Since the dataset does not include user IDs or explicit star ratings, I used
implicit signals such as review text, sentiment, review count, and theme
features.

One limitation is that the sentiment method is lexicon-based, so it may not fully
understand sarcasm or complex mixed opinions.

Another limitation is that collaborative filtering cannot be properly applied
without user IDs or ratings.

In the future, this system can be improved by using a trained sentiment model,
adding user ratings, adding maps and coordinates, including travel-time features,
and adding seasonality or traveller-profile filters.

To conclude, this project demonstrates how Apache Spark can be used for a full
Big Data Analytics workflow: loading and cleaning raw data, engineering features,
running large-scale aggregations, generating insights, and building a practical
recommendation system.

Thank you.

## Short Demo Flow After Slides

If I need to show the system demo, I will do it in this order:

1. Open `tourism_project.py` and briefly show the Spark session, data loading,
   cleaning, feature engineering, analytics, and recommendation functions.
2. Run the project using:

```powershell
.\venv\Scripts\python.exe tourism_project.py
```

3. Show the terminal outputs: dataset summary, top destinations, district
   recommendations, theme hotspots, and Riverston recommendations.
4. Open `demo_ui.html` and show the dashboard sections:
   summary, demand analytics, district recommender, theme hotspots, hybrid
   similarity recommender, and generated charts.

## Very Short Version

If I only have around 3 minutes, I can say:

This project uses Apache Spark to analyze Sri Lankan tourism destination
reviews. I loaded and cleaned more than 35,000 review records, created sentiment
and travel-theme features, and used Spark aggregations to identify popular
destinations, strong districts, and theme-based hotspots.

For recommendations, I implemented three views. The first recommends top
destinations by district. The second recommends destinations by travel theme.
The third is a hybrid content-based recommender that uses Spark ML TF-IDF and
cosine similarity, then combines similarity with sentiment quality and
popularity confidence.

The main strength of the project is that it does not simply rank by review
count. It uses a confidence-weighted score with Bayesian positive rate,
popularity confidence, review engagement, and theme diversity. The project also
includes a dashboard UI to present the analytics and recommendation outputs
clearly.
