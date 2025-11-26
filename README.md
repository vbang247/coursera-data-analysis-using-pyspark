# Data Analysis Using PySpark

A comprehensive data analysis project demonstrating distributed data processing capabilities using PySpark on large-scale music listening data from Last.fm.

## 📋 Project Overview

This project is part of the [Coursera Guided Project: Data Analysis Using PySpark](https://www.coursera.org/projects/data-analysis-using-pyspark). The project focuses on analyzing music listening patterns from Last.fm, an online music service, using Apache Spark for distributed data processing.

## 🎯 Learning Objectives

- Setup and configure distributed data processing environment
- Apply various SQL queries to extract meaningful insights from large datasets
- Perform data cleaning and transformation operations
- Join multiple datasets for advanced analysis
- Visualize query results using matplotlib

## 🛠️ Technologies & Skills

### Technologies
- **PySpark** - Distributed data processing framework
- **Apache Spark** - Big data processing engine
- **Python** - Programming language
- **Matplotlib** - Data visualization
- **Pandas** - Data manipulation (for visualization)

### Skills Practiced
- Distributed Computing
- Big Data Processing
- Data Analysis
- Data Visualization
- Data Management
- Data Cleansing
- Query Languages
- Data Manipulation
- Data Processing

## 📊 Dataset

The project uses two CSV files from Last.fm:

1. **listenings.csv** (~1 GB)
   - Contains user listening history
   - Columns: `user_id`, `date`, `track`, `artist`, `album`
   - Millions of records

2. **genre.csv** (~3 MB)
   - Contains artist genre classifications
   - Columns: `artist`, `genre`
   - Maps artists to their respective music genres

## 🔍 Key Analyses Performed

### 1. Data Exploration & Cleaning
- Schema inspection and data profiling
- Handling missing values (null handling)
- Data shape analysis

### 2. User Behavior Analysis
- **Query 1**: Find all records of users who listened to specific artists (e.g., Rihanna)
- **Query 2**: Identify top 10 users who are fans of specific artists
- **Query 6**: Find top 10 users who are fans of pop music
- **Query 8**: Determine each user's favorite genre

### 3. Music Popularity Analysis
- **Query 3**: Find top 10 most streamed tracks across all artists
- **Query 4**: Find top 10 most popular tracks by specific artists (e.g., Rihanna)
- **Query 7**: Identify top 10 most popular music genres

### 4. Album Analysis
- **Query 5**: Find top 10 most popular albums

### 5. Genre Analysis
- **Query 9**: Count artists by genre (pop, rock, metal, hip hop)
- Genre distribution visualization using matplotlib

### 6. Data Integration
- Inner join between listening data and genre data
- Combined analysis of listening patterns with genre information

## 📈 Key Insights

- **Most Streamed Track**: "Sorry" by Justin Bieber (3,381 streams)
- **Most Popular Genre**: Rock (2,691,934 listens)
- **Top Album**: "The Life Of Pablo" by Kanye West (22,310 listens)
- **Genre Distribution**: Rock dominates with over 2.6M listens, followed by Pop with 1.5M listens

## 🚀 Getting Started

### Prerequisites
- Python 3.x
- Apache Spark
- PySpark
- Matplotlib
- Pandas

### Installation

```bash
# Install required packages
pip install pyspark matplotlib pandas
```

### Running the Project

1. **Setup Spark Environment**
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()
   ```

2. **Load the Dataset**
   ```python
   listening_df = spark.read.csv('listenings.csv', inferSchema=True, header=True)
   genre_df = spark.read.csv('genre.csv', inferSchema=True, header=True)
   ```

3. **Run the Notebook**
   - Open `coursera_data_analysis_notebook.ipynb` in your Jupyter environment
   - Execute cells sequentially to perform the analysis

### Note
The original project was completed in Google Colab with Databricks runtime. For local execution, ensure you have Spark properly configured.

## 📁 Project Structure

```
coursera-data-analysis-using-pyspark/
├── README.md
├── coursera_data_analysis_notebook.ipynb
├── listenings.csv
├── genre.csv
└── dataset.zip
```

## 🎓 Certification

This project was completed as part of the Coursera Guided Project: **Data Analysis Using PySpark** by Ahmad Varasteh.
View certification: ['link'](https://www.coursera.org/account/accomplishments/records/7IOEWG1NMR0E)

## 💡 Key Takeaways

1. **Distributed Processing**: Learned to handle large datasets (1GB+) efficiently using Spark's distributed computing capabilities
2. **Data Joins**: Successfully joined multiple datasets to enrich analysis
3. **Query Optimization**: Applied various PySpark SQL functions for efficient data extraction
4. **Visualization**: Converted Spark DataFrames to Pandas for visualization using matplotlib
5. **Real-world Application**: Analyzed real music streaming data to extract business insights

## 🔗 Links

- [Coursera Project Page](https://www.coursera.org/projects/data-analysis-using-pyspark)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

## 📝 License

This project is for educational purposes as part of the Coursera course.

---

**Author**: Varsha Ratankumar Bang  
**Date**: 2025  
**Status**: ✅ Completed
