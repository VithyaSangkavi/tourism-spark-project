# Sri Lanka Tourism Big Data Analytics and Recommendation System

This project was built for the Big Data Analytics mini project assignment. It uses
PySpark to analyze Sri Lankan tourism destination reviews, discover tourism
demand patterns, and generate recommendation outputs for a dashboard-style demo.

## Project Scope

The implementation covers both assignment parts:

- Part A: big data analytics using Apache Spark
- Part B: hybrid tourism recommendation system using Spark ML feature engineering

## Dataset

Dataset file:

```text
data/Destination Reviews_(raw).csv
```

Columns:

- `Destination`
- `District`
- `Timespan`
- `Review`

The raw dataset contains noisy review text and some malformed rows. The project
cleans the data by keeping valid districts, removing missing values, normalizing
text, and filtering invalid records.

## Main Features

- Spark CSV loading with multiline and quoted review support
- Data cleaning and preprocessing
- Review volume analytics by destination and district
- Lexicon-based sentiment scoring
- Travel theme extraction for nature, beach, culture, family, adventure, relaxation, and general experiences
- Confidence-weighted destination quality ranking using sentiment, popularity, review engagement, and theme diversity
- District-level demand and opportunity analytics
- Theme hotspot analytics with Bayesian positive-rate smoothing
- Top destination recommendations by district
- TF-IDF destination profiles using Spark ML
- Hybrid content-based recommendation using cosine similarity, sentiment quality, and popularity confidence
- Dashboard JSON export for the demo UI
- Output charts for presentation slides and the dashboard

## How to Run

Create or activate a Python environment with the required packages:

```bash
pip install -r requirements.txt
```

Open and run the main assignment notebook:

```text
tourism_project.ipynb
```

The notebook is self-contained and includes the full PySpark implementation.

Generated charts are saved in:

```text
outputs/
```

The notebook also exports:

```text
outputs/dashboard_data.json
```

## Demo UI

For the system demonstration video, open this file in a browser:

```text
demo_ui.html
```

The UI summarizes the Spark analytics, district recommendations, theme hotspots,
hybrid similarity recommendations, and generated charts in one screen-friendly
dashboard.

## Important Outputs

The project prints:

- Clean dataset row count
- Destination and district counts
- Top destinations by review volume
- Overall sentiment distribution
- Best destinations using confidence-weighted recommendation score
- Top 3 destinations per district
- District-level opportunity scores
- Theme-based tourism hotspots
- District-based recommendations
- Hybrid content-based recommendations similar to a selected destination
- Recommendation coverage