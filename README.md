# Reed-Data-Engineer-Job-ETL-Pipeline
"Reed Data Engineer Job ETL Pipeline" is an automated ETL that extracts job listings for Data Engineer positions from Reed's job portal. The pipeline transforms the raw data by filtering and organizing job details such as job title, company name, location, salary, job type, link and then loads the structured information into a database.
# Objective :
To extract job listings related to data engineering from Reed, process the data to extract useful information, and store it in a structured format.
# Data source : 
The data is sourced from Reed's job listing portal, using web scraping techniques to gather job postings.
# ETL Process :
**Extract** : The pipeline scrapes job listings related to Data Engineer roles, capturing relevant details such as job title, company name, location, salary, job type, and the job listing link. 

**Transform** : The extracted data undergoes cleaning. Specific cleaning methods include:
1. Removing duplicate values.
2. Removing NaN values.
**Load** : The cleansed data is loaded into a SQL database, where it is stored in a structured format.

