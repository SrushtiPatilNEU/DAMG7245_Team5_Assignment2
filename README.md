# Assignment-2

## Live application Links
[![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=10LyLUw6ExvnydJ-WWU9VY5ZdL0ZN6cSuQ4gK6o7-hZo#0)

- Fast Api : https://damg7245-team5-assignment2.onrender.com
- Streamlit Application: https://damg7245team5assignment2-duhefdi76eqengrappe588s.streamlit.app/
- video : https://youtu.be/SFfq909AHa0

## Problem Statement 
In this project, SEC Financial Statement Data will be extracted, validated, and stored in Snowflake utilising a completely automated data pipeline built with Airflow. It entails creating two FastAPI services: one that interfaces with Snowflake to run and provide query replies, and another that accepts SEC data connections and initiates an Airflow pipeline for extraction, transformation, and loading into Snowflake. A Streamlit-based application that enables users to choose financial datasets, start the pipeline for processing, and extract structured data from Snowflake for analysis is part of the workflow.
 
## Project Goals
Develop FastAPI services for handling data input and triggering the Airflow pipeline.
Automate data extraction from SEC files, process metadata, and load it into Snowflake.
Create a user-friendly Streamlit interface for viewing extracted financial data.
Containerize all services using Docker for easy deployment.

## Technologies Used
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)
[![FastAPI](https://img.shields.io/badge/fastapi-109989?style=for-the-badge&logo=FASTAPI&logoColor=white)](https://fastapi.tiangolo.com/)
[![Amazon AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-%232496ED?style=for-the-badge&logo=Docker&color=blue&logoColor=white)](https://www.docker.com)
[![Snowflake](https://img.shields.io/badge/snowflake-%234285F4?style=for-the-badge&logo=snowflake&link=https%3A%2F%2Fwww.snowflake.com%2Fen%2F%3F_ga%3D2.41504805.669293969.1706151075-1146686108.1701841103%26_gac%3D1.160808527.1706151104.Cj0KCQiAh8OtBhCQARIsAIkWb68j5NxT6lqmHVbaGdzQYNSz7U0cfRCs-STjxZtgPcZEV-2Vs2-j8HMaAqPsEALw_wcB&logoColor=white)
](https://www.snowflake.com/en/?_ga=2.41504805.669293969.1706151075-1146686108.1701841103&_gac=1.160808527.1706151104.Cj0KCQiAh8OtBhCQARIsAIkWb68j5NxT6lqmHVbaGdzQYNSz7U0cfRCs-STjxZtgPcZEV-2Vs2-j8HMaAqPsEALw_wcB)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)](https://streamlit.io/)

## Pre requisites
1. Python Knowledge
2. Snowflake Account
3. AWS S3 bucket
4. Docker Desktop
5. FastaAPI knowledge
6. MongoDB database knowledge
7. Postman knowledge
8. Stremlit implementation
9. Airflow pipeline knowledge
10. Google Cloud Platform account and hosting knowledge


## How to run Application Locally
1️⃣ Clone the Repository
git clone <repository_url>
cd sec-data-pipeline

2️⃣ Set Up Virtual Environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

3️⃣ Install Dependencies
pip install -r requirements.txt

4️⃣ Configure Environment Variables
Create a .env file in the root directory and add:
AWS_ACCESS_KEY=<your_access_key>
AWS_SECRET_KEY=<your_secret_key>
SNOWFLAKE_USER=<your_username>
SNOWFLAKE_PASSWORD=<your_password>

5️⃣ Run FastAPI Backend
cd backend
uvicorn main:app --reload
API will be available at http://127.0.0.1:8000

6️⃣ Start Airflow Pipeline
docker-compose up --build
Airflow UI will be available at http://localhost:8080

7️⃣ Start Streamlit Frontend
cd frontend
streamlit run app/main.py


## Project run outline

CodeLab - (https://codelabs-preview.appspot.com/?file_id=10LyLUw6ExvnydJ-WWU9VY5ZdL0ZN6cSuQ4gK6o7-hZo#0)

## References
Airflow Documentation
Snowflake Docs
Streamlit Documentation
FastAPI Guide

  
  Name | Contribution %|
  --- |--- |
Pranjal Mahajan(002375449)  | 33.33% | 
 Srushti Patil (002345025)   | 33.33% | 
 Ram Putcha (002304724) | 33.33% |
