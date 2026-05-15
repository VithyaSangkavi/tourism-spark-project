# Presentation Outline

Use this as a maximum 10-slide structure for the assignment presentation.

## Slide 1: Title

Sri Lanka Tourism Big Data Intelligence and Recommendation System

## Slide 2: Problem and Objective

- Analyze tourist review data using Apache Spark
- Identify tourism demand, destination quality, and district opportunity
- Recommend destinations based on district preference, travel theme, and review similarity

## Slide 3: Dataset

- Dataset: `Destination Reviews_(raw).csv`
- Main columns: Destination, District, Timespan, Review
- Contains more than 35,000 cleaned review records
- Covers 236 tourism destinations across 12 districts

## Slide 4: Data Processing With Spark

- Loaded CSV using PySpark
- Handled quoted and multiline review text
- Removed missing and invalid records
- Normalized review text for text analytics
- Filtered malformed district values
- Created sentiment, word engagement, recency, and travel theme features

## Slide 5: Part A Analytics

- Review count by destination and district
- Sentiment distribution
- Confidence-weighted destination ranking
- District opportunity analytics
- Theme-based tourism hotspots

## Slide 6: Key Analytics Findings

- Matale, Colombo, Hatton, and Matara have the highest review activity
- Most reviews are positive or neutral
- Strong destinations combine positive sentiment, popularity confidence, review engagement, and theme diversity

## Slide 7: Recommendation Methods

- District-based recommendation ranks destinations within a selected district
- Theme-based recommendation highlights nature, beach, culture, family, adventure, and relaxation hotspots
- Hybrid content-based recommendation uses Spark ML TF-IDF, cosine similarity, sentiment, and popularity

## Slide 8: Sample Recommendations

- Colombo district recommendations include Beddagana Wetland Park, Diyatha Uyana, and Mount Lavania Beach
- Riverston similarity recommendations include Pitawala Pathana, Bambarakiri Ella, Sembuwatta Lake, and Horton Plains
- Theme examples include Mirissa Beach for beach travel and Sembuwatta Lake or Riverston for relaxation

## Slide 9: Evaluation and Limitations

- Recommendation coverage: 236 destination profiles
- Uses implicit signals because explicit user ratings are unavailable
- Sentiment is lexicon-based, so sarcasm and complex opinions may not be detected
- Bayesian smoothing reduces the risk of over-ranking destinations with very small review samples

## Slide 10: Future Improvements

- Add trained sentiment classification
- Use user ratings if available for collaborative filtering
- Add maps, coordinates, and travel-time features
- Add seasonality and traveller profile filters
- Deploy the dashboard as a full web app with live Spark output loading
