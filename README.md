# Chicago Crimes Analysis with Apache Spark on Google Cloud Platform

## Overview

This project analyzes crime data from Chicago using **Apache Spark**. The data includes information about various types of crimes, their locations, times, and other relevant attributes. The goal is to extract meaningful insights, such as hotspots for specific crimes, crime trends by area, and the most common hours for criminal activities.

The jobs are executed on a **Google Cloud Platform (GCP)** cluster with **one master node and two worker nodes**, enabling distributed and efficient data processing.

---

## Data Description

The dataset contains structured data on crimes reported in Chicago, with columns representing:

- **Crime Type**: Classification of the crime (e.g., theft, assault).
- **Location**: Geographical area or district.
- **Timestamp**: Date and time of the crime.
- **Other Attributes**: Additional details such as crime description and resolution status.

This data provides the foundation for identifying patterns, trends, and correlations within Chicago's crime statistics.

---

## Project Structure

The source code is organized into specific Spark jobs, each focusing on a particular aspect of the analysis:

### 1. **Transform Columns (********`transform_columns.scala`********)**

- Cleans and standardizes column names and data types.
- Performs initial preprocessing to prepare the dataset for further analysis.

### 2. **Commun Utilities (********`util.scala`********)**

- Contains reusable utility functions and helper methods used across multiple jobs.

### 3. **Community Type by District (********`commun_type_by_district.scala`********)**

- Aggregates and categorizes crime data by district.
- Identifies the most prevalent types of crimes for each district.

### 4. **Crimes Rate by Area (********`crimes_rate_by_area.scala`********)**

- Calculates the crime rate per area or district.
- Provides insights into high-crime and low-crime areas.

### 5. **Job 1 (********`job1.scala`********)**

- General-purpose job for analyzing and testing specific aspects of the data.
- Serves as a template or starting point for new analyses.

### 6. **Kidnapping and Robbery Hotspots (********`kidnappin_robbery_hotspots.scala`********)**

- Identifies areas with the highest rates of kidnappings and robberies.
- Useful for understanding and mitigating high-risk zones.

### 7. **Most Criminal Hour (********`most_criminal_hour.scala`********)**

- Analyzes timestamps to find the hours during which crimes are most likely to occur.
- Provides insights into temporal crime patterns.

---

## Execution Environment

- The jobs are executed on a **GCP cluster** with:
  - **1 master node**: Responsible for coordinating the Spark jobs.
  - **2 worker nodes**: Perform distributed data processing.
- This setup ensures high performance and scalability for handling large datasets.

---

## Insights and Outcomes

By leveraging Spark’s distributed processing capabilities and GCP’s scalable infrastructure, this project:

- Identifies crime hotspots and high-risk districts.
- Highlights temporal patterns, such as peak crime hours.
- Provides actionable insights for law enforcement and policymakers.

---

## Next Steps

- Integrate advanced visualizations using Looker Studio.
- Expand the analysis to include predictive modeling using Spark MLlib.
- Automate the pipeline using GCP tools like Dataflow or Cloud Composer.

