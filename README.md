# F1 Quali Comparison

[f1teammatequali.com](https://www.f1teammatequali.com)

A web app that allows users to evaluate a driver's qualifying performance. Users can compare how well a driver did:
  - compared to their teammate across a season
  - compared to their teammate across all races they took part in together
  - compared to their current and past teammates at a particular circuit
  - compared to the fastest driver across a season

## Data Pipeline
![Project Pipeline](https://github.com/JaiChandak/F1_quali_comparison/blob/main/images/f1_quali_web_app.png)
1. **Jolpica API**: Data source
2. **Airflow**: Orchestrates the ETL process
3. **S3**: Data storage (raw)
4. **Lambda**: Tasked to insert data from csv to PostgreSQL database
5. **RDS**: Data storage (cleaned)
6. **Beanstalk**: Hosting the Flask web app that users can interact with

## Data Model
![Data Model](https://github.com/JaiChandak/F1_quali_comparison/blob/main/images/sql_data_model.PNG)
