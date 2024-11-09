# Vishesh_Gupta_IDS706_Week10

[![CI](https://github.com/nogibjj/Vishesh_Gupta_IDS706_Week10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Vishesh_Gupta_IDS706_Week10/actions/workflows/cicd.yml)

## Soccer Match Data Analysis

This project loads, processes, and analyzes soccer match data for various operations, including loading, describing, querying, and transforming data. The analysis covers team scores, results, and basic statistics from Premier League matches.

## Project Overview

This project performs data operations on a dataset containing English Premier League match results from the 2019-2020 season. Operations include loading and inspecting raw data, generating summaries, querying specific match rounds, and transforming data to extract additional insights like match results for each team.

## Setting Up Spark

You can use one of the following methods to set up Spark:

1. **Install Spark Locally**: Download and install [Apache Spark](https://spark.apache.org/downloads.html) on your local machine. Ensure that you add Spark to your system PATH to access `spark-submit` and `pyspark` commands.

2. **Use GitHub Codespaces**: Codespaces offers a convenient Linux-based development environment that typically includes a pre-installed version of Spark. You can use this to get started without local installation. Professor Gift's Ruff template for Codespaces provides an ideal setup.

## Data Loading

The dataset contains records of individual soccer matches. Sample data structure:

| Round | Date            | Team 1               | FT   | Team 2                     |
|-------|------------------|----------------------|------|-----------------------------|
| 1     | Fri Aug 9 2019   | Liverpool FC         | 4-1  | Norwich City FC            |
| 1     | Sat Aug 10 2019  | West Ham United FC   | 0-5  | Manchester City FC         |

### Operations
- **Load Data**: Load match data from the source file.
- **Display Sample**: Display the first few rows to verify loading.

## Data Description

After loading, basic statistics are generated to summarize the dataset.

| Summary | Round | Date           | Team 1                     | FT   | Team 2                     |
|---------|-------|----------------|----------------------------|------|-----------------------------|
| Count   | 380   | 380            | 380                        | 380  | 380                         |
| Mean    | 19.5  |                |                            |      |                             |
| Stddev  | 10.98 |                |                            |      |                             |
| Min     | 1     | Fri Aug 23 2019| AFC Bournemouth            | 0-0  | AFC Bournemouth             |
| Max     | 38    | Wed Jun 24 2020| Wolverhampton Wanderers FC | 8-0  | Wolverhampton Wanderers FC  |

## Data Querying

Specific queries allow extraction of match details, such as querying by match round.

Example query:
```sql
SELECT * FROM MatchData WHERE Round = 1;
```

| Round | Date            | Team 1               | FT   | Team 2                     |
|-------|------------------|----------------------|------|-----------------------------|
| 1     | Fri Aug 9 2019   | Liverpool FC         | 4-1  | Norwich City FC            |
| 1     | Sat Aug 10 2019  | West Ham United FC   | 0-5  | Manchester City FC         |

## Data Transformation

The transformation step adds columns to the dataset, including:
- **Team1_Score**: Score for Team 1
- **Team2_Score**: Score for Team 2
- **Result**: Outcome for Team 1 (Win, Loss, Draw)

| Round | Date            | Team 1               | FT   | Team 2                     | Team1_Score | Team2_Score | Result |
|-------|------------------|----------------------|------|-----------------------------|-------------|-------------|--------|
| 1     | Fri Aug 9 2019   | Liverpool FC         | 4-1  | Norwich City FC            | 4           | 1           | Win    |
| 1     | Sat Aug 10 2019  | West Ham United FC   | 0-5  | Manchester City FC         | 0           | 5           | Loss   |

Detailed information can be found in **final_pyspark_output.md** in this repository

