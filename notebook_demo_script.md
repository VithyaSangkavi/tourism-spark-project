# Notebook Demo Script

Project: Sri Lanka Tourism Big Data Intelligence and Recommendation System

Estimated demo duration: 5 to 7 minutes

## Before Starting

Open these files before recording or presenting:

- `tourism_project.ipynb`
- `demo_ui.html`

Also make sure the virtual environment is ready. If needed, run:

```powershell
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 1. Introduce the Notebook

Say:

Hello, this is my Big Data Analytics project notebook. The project is titled
**Sri Lanka Tourism Big Data Intelligence and Recommendation System**.

In this notebook, I run the full PySpark pipeline for tourism review analytics
and recommendation generation.

The notebook is self-contained, so the important code for the assignment is
inside the `.ipynb` file itself. I also kept a `.py` file in the folder as a
backup/helper version, but for this assignment I will present and submit the
notebook.

## 2. Show the Project Summary Cell

Action:

Scroll to the first markdown cell in `tourism_project.ipynb`.

Say:

This notebook summarizes the project workflow. The project includes Spark data
ingestion, data cleaning, sentiment processing, feature engineering, destination
analytics, district recommendations, theme-based recommendations, and TF-IDF
content-based recommendations.

The dataset contains tourist reviews about Sri Lankan destinations. After
cleaning, the system works with more than 35,000 valid review records and 236
destination profiles.

## 3. Explain Why Spark Is Used

Say:

Since this is a Big Data Analytics assignment, I used Apache Spark through
PySpark instead of only Pandas.

Spark is suitable because it supports distributed-style processing, DataFrame
transformations, SQL-style aggregations, window functions, and machine learning
feature extraction through Spark ML.

In this project, Spark is used for:

- Loading the raw CSV dataset
- Cleaning and filtering records
- Creating text features
- Aggregating destination and district insights
- Ranking recommendations
- Building TF-IDF features for content similarity

## 4. Run the Notebook Code Cell

Action:

Run the notebook cells from top to bottom, especially the final cell named
`Run the Full Pipeline`.

Say while it starts:

Now I will run the full project pipeline from the notebook.

The notebook starts by creating a Spark session, then loads the raw CSV file using
Spark read options such as header support, multiline support, quote handling,
escape handling, and encoding support.

These options are important because review text can contain commas, quotation
marks, and multiple lines.

## 5. Explain the Raw Schema Output

Action:

When the schema appears, point to the output.

Say:

Here, Spark prints the raw schema. The main columns are:

- Destination
- District
- Timespan
- Review

This confirms that the dataset has loaded correctly into a Spark DataFrame.

## 6. Explain Data Cleaning Output

Action:

When the dataset summary appears, point to the clean row count, destinations,
and districts.

Say:

After loading the raw data, the project applies cleaning steps.

The cleaning process removes rows with missing destination, district, or review
values. It also keeps only valid districts, removes very short reviews, converts
text to lowercase, removes punctuation, and normalizes extra spaces.

After cleaning, the dataset contains around **35,509 clean review records**,
**236 destinations**, and **12 valid districts**.

The district count table shows which districts have the highest amount of review
activity. For example, Matale, Colombo, Hatton, and Matara have high review
volumes.

## 7. Explain Sentiment and Feature Engineering

Say:

Before analytics, the system creates several review-level features.

First, it performs lexicon-based sentiment analysis. The system checks for
positive words such as good, beautiful, amazing, peaceful, and excellent. It
also checks for negative words such as bad, poor, dirty, unsafe, and worst.

Based on these terms, each review is classified as positive, neutral, or
negative.

Second, it creates review engagement features using word count.

Third, it creates recency segments from the Timespan column.

Finally, it classifies reviews into travel themes such as nature, beach,
culture, family, adventure, relaxation, and general.

These features are used later for ranking and recommendation.

## 8. Explain Part A Analytics

Action:

When the output reaches `Part A: Spark Analytics`, point to each table.

Say:

This section is Part A of the assignment: Big Data Analytics using Spark.

First, Spark groups the data by destination and district to find the top
destinations by review volume.

Next, it calculates the overall sentiment distribution. This shows how many
reviews are positive, neutral, or negative.

Then the system builds a destination quality table. This is more advanced than
only counting reviews.

The destination score combines:

- Bayesian positive rate
- Popularity confidence
- Review engagement
- Theme diversity

This makes the ranking more reliable because a destination should be popular,
positively reviewed, and supported by enough review evidence.

## 9. Explain Top Recommendations by District

Action:

Point to the `Top 3 recommended destinations per district` output.

Say:

This table shows the top three recommended destinations for each district.

Spark uses a window function to rank destinations within each district. This is
useful because a tourist may already know the district they want to visit, and
the system can recommend the best destinations in that area.

For example, in Colombo, the system recommends destinations such as Beddagana
Wetland Park, Diyatha Uyana, and Mount Lavania Beach.

## 10. Explain District Opportunity Insights

Action:

Point to the district-level demand and opportunity table.

Say:

This table gives district-level analytics.

For each district, the system calculates total review count, destination count,
positive review count, negative review count, positive rate, reviews per
destination, and opportunity score.

This is useful for tourism planning because it shows not only which districts
already have high demand, but also which districts may have room for growth.

## 11. Explain Theme-Based Hotspots

Action:

Point to the theme hotspot output.

Say:

This section shows theme-based tourism hotspots.

The system groups destinations by travel theme, such as nature, beach, culture,
family, adventure, and relaxation.

Then it ranks destinations inside each theme using a theme score.

I used Bayesian smoothing here as well, because otherwise a destination with
only one or two positive reviews could unfairly rank at the top.

This makes the recommendation system more personalized because different
travellers may prefer different types of experiences.

## 12. Explain Part B Recommendation System

Action:

When the output reaches `Part B: Recommendation System`, point to the district
recommendation and Riverston similarity output.

Say:

This section is Part B of the assignment: the recommendation system.

The first recommendation method is district-based recommendation. Here, the
system recommends the strongest destinations in a selected district.

The second method is content-based recommendation using review text.

For the content-based recommender, Spark groups all reviews for each destination
into one destination profile. Then Spark ML is used to apply:

- Tokenizer
- StopWordsRemover
- HashingTF
- IDF

These steps create TF-IDF vectors for each destination.

Then the system calculates cosine similarity between the selected destination
and other destinations.

In this example, the selected destination is Riverston, and the system
recommends similar places such as Pitawala Pathana, Horton Plains National Park,
Sembuwatta Lake, and Bambarakiri Ella.

The final hybrid score also includes sentiment quality and popularity
confidence, so the recommendations are not based only on text similarity.

## 13. Explain Generated Outputs

Action:

Scroll to the end of the notebook output.

Say:

At the end, the project saves output files into the `outputs` folder.

These include:

- Top destination chart
- Sentiment distribution chart
- District strength chart
- Dashboard data JSON file

The dashboard data JSON is used by the demo UI to present the results in a more
visual and interactive way.

## 14. Open the Demo UI

Action:

Open `demo_ui.html` in a browser.

Say:

Now I will open the dashboard UI created for the project.

This dashboard presents the Spark results in a clearer way. It includes the
summary metrics, demand analytics, district recommender, theme hotspots, hybrid
similarity recommender, and generated visualizations.

This makes the project look like an analytics product rather than only a console
or notebook output.

## 15. Show the Dashboard Sections

Action:

Scroll through the dashboard.

Say:

At the top, the dashboard shows the project summary and the hybrid ranking
formula.

The analytics section shows the most reviewed destinations and quality ranking
signals.

The district recommender lets the user select a district and view the top
recommended destinations.

The theme hotspot section allows the user to switch between travel themes such
as nature, beach, relaxation, family, culture, adventure, and general.

Finally, the hybrid similarity recommender shows destinations similar to
Riverston, based on TF-IDF text similarity combined with sentiment and
popularity confidence.

## 16. Conclusion

Say:

In conclusion, this project demonstrates a complete Big Data Analytics workflow
using Spark.

It starts from raw tourism review data, performs cleaning and preprocessing,
creates useful features, runs analytics, and builds a recommendation system.

The main strength of the project is that the recommendation logic is not based
only on review count. It uses confidence-weighted scoring, theme extraction, and
TF-IDF similarity to produce more meaningful recommendations.

Thank you.

## Quick Version for a Short Demo

If the demo time is limited, use this shorter version:

I am running the project through `tourism_project.ipynb`. The notebook contains
the main PySpark pipeline for the assignment.

The project starts by loading the raw tourism review CSV using Spark with
multiline and quote handling. Then it cleans the data by removing missing
values, invalid districts, short reviews, and noisy text.

After cleaning, the system has more than 35,000 valid review records and 236
destination profiles.

Next, the project creates sentiment features, review engagement features,
recency segments, and travel themes. Spark then aggregates the data to find top
destinations, district insights, sentiment distribution, and theme-based
hotspots.

For recommendations, the project uses three approaches: district-based ranking,
theme-based ranking, and hybrid content-based recommendation. The content-based
method uses Spark ML TF-IDF features and cosine similarity, then combines
similarity with sentiment and popularity confidence.

Finally, the project generates charts and dashboard data, which are shown in the
demo UI.
