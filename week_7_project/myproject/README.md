# Memphis Police Data, 1986 to Present

For my capstone project for Data Engineering Zoomcamp 2023, I created an end-to-end data pipeline. The pipeline runs daily to make calls to an API.

## Problem Description

The city of Memphis, TN has an [open-source data hub]([url](https://data.memphistn.gov/)). 
Specifically, this project uses the [Public Safety Incidents dataset]([url](https://data.memphistn.gov/Public-Safety/Memphis-Police-Department-Public-Safety-Incidents/ybsi-jur4)) from the Memphis Police Department.
This dataset includes cases from 1986 to present and is updated daily.


It's important to note that police data doesn't give a complete picture of perceptions of safety. Due to mistrust
of the police, people may refrain from calling the police when a safety threat comes along (ADD CITATION). 
Police may also use pretextual traffic stops, where they stop people for minor traffic violations that are not real
threats to safety in order to search them.
When advocating for criminal justice reform that emphasizes non-carceral ways of maintaining public safety, it's important to understand 
how the police are spending their time and what types of crimes are being committed. 


This project uses Memphis Police data to answer the following questions:

- How many crimes have been reported to police in Memphis, TN?
- What is the distribution of different crime categories (e.g., Motor Vehicle Theft, Assault, etc.)?

Because this is a large dataset that gets updated daily, this project also presents an automated solution to ingest only the latest data each day
to allow for effective data processing and a regularly updated dashboard.


## About the Pipeline 

The pipeline uses the following technologies:
- Terraform to create Google Cloud resources
- Google Cloud Storage to serve as a data lake for storing raw data
- BigQuery to serve as a data warehouse where the data is prepared for analysis
- 

## Instructions for Replication

To replicate this project, you'll need a Google Cloud Platform account. GCP offers a 30-day free trial. 

## Future Considerations

Future iterations of this project may consider 

