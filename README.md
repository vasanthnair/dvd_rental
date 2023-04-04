

## **Project Name: DVD Rental ELT Project Using Airbyte and Databricks**

## **Team Name: GROUP 7**

 ## **Document Version 1**

**DATE : Apr 4, 2023**

## **Revision History**

| **Version** | **Authors**           | **Date**  | **Description** |
|-------------|-----------------------|-----------|-----------------|
|      1.0    |**Vasanth Nair**       |Apr/04/2023|                 |
|      1.0    |**Daniel Marinescu**   |Apr/04/2023|                 |

## **Index**
<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
   <li>
      <a href="#goals">Goals</a>
    </li>
   <li>
      <a href="#project-context">Project Context</a>
    </li>
   <li>
      <a href="#architecture">Architecture</a> 
     <ul>
        <li><a href="#dvd-rental-pipeline-steps">DVD Rental Pipeline Steps</a></li>
      </ul>
   </li>
   <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
      </ul>
      <ul>
        <li><a href="#setup">Setup</a></li>
      </ul>
      <ul>
        <li><a href="#airbyte-integration">Airbyte Integration</a></li>
      </ul>
      <ul>
        <li><a href="#databricks-transformations-and-testing">Databricks Transformations and Testing</a></li>
      </ul>
      <ul>
        <li><a href="#powerbi---semantic-layer-and-reporting">PowerBI - Semantic layer and reporting</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project


This project aims to build a scalable and efficient ELT pipeline that extracts the DVD sample database maintained in an RDS table, performs transformations and tests in in Databricks, and presents visualizations and metrics in Power BI. The project requires a solution that can handle large amounts of data while ensuring the integrity and accuracy of the information. The ELT should be hosted on AWS, taking advantage of its various services to ensure the robustness and reliability of the pipeline. 



### Built With

The team used a variety of tools in this project, `Python`, `Git`,`AWS`, `Databricks` and `PowerBI`.


* ![Python](https://a11ybadges.com/badge?logo=python) -- **`Python`** was used for developing custom scripts to perform data transformations and manipulation. Its powerful libraries, such as Pandas and NumPy, were utilized for data manipulation.


* ![Git](https://a11ybadges.com/badge?logo=git)  -- **`Git`** was used for version control to manage the codebase and collaborate with other team members.


* ![AWS](https://a11ybadges.com/badge?logo=amazonaws) -- **`AWS`** was used as the cloud platform to host the applications, store, and leverage various services for data hosting.


* ![Databricks](https://a11ybadges.com/badge?logo=databricks) -- **`Databricks`** to perform transformations as the data is moved from bronze, to silver, to gold layers. Great Expectations test were also performed in the framework.


* ![PowerBI](https://a11ybadges.com/badge?logo=powerbi) -- **`PowerBI`** was used for the semantic, reporting layer of the project to illustrate data visualization and present metrics.



<!-- GOALS -->
## Goals

<p>Building a data pipeline solution to provide a system that can process and deliver DVD rental data to BI and upper management consumers as quickly as possible. By leveraging real-time data processing, data can be continuously ingested and transformed, ensuring that the data is always up-to-date and available to data consumers with minimal latency. This pipeline solution allows BI and Upper Management consumers to make use of DVD rental data, which is especially important in fast-paced rental business environments.</p>

<p>Our solution ensures scalability which can handle large amounts of data with minimal impact on performance. Overall, building a live and incremental data pipeline solution aims to create a scalable, reliable, and high-performing data processing system that enables BI & Upper Management data consumers to make informed decisions or perform complex analysis.</p>



<!-- PROJECT CONTEXT -->
## Project Context

<p>Business intelligence at a DVD rental company requires access to an structured and clean layer of data, on a scalable fashion for business analysis. In addition, Upper Management requires a presentation reporting framework that visualizes business metrics, in order to make proper business decisions. Our Airbyte-to-Databricks solution allows to meet these needs of both parties, in terms of decision-making or analysis.
</p>




<!-- ARCHITECTURE -->
## Architecture

![image](https://user-images.githubusercontent.com/29958551/229655848-7cf7d55e-59b1-4631-9574-7ddccb6359d3.png)


### DVD Rental Pipeline Steps

1. Airbyte is booted locally and downloads the dvd_rental data from RDS tables to the DBFS of an AWS-hosted Databricks cluster.
2. The Databricks workflow `dvd_rental_workflow_with_tests` runs which executes all the notebooks containing the transformations from bronze layer, to silver layer, to gold layer data.
3. The Databricks workflow also runs notebooks containing Great Expectations tests which verify via assertions the integrity of the data as it moves from layer to layer.
4. A PowerBI dashboard file queries the gold layer table `gold_dim_customer`, `gold_dim_film`, gold_dim_location`, `gold_fact_transaction`, `gold_obt` in a PowerBI hosted dimensional model.
5. The PowerBI dashboard updates with overall financial reporting on the `General` tab, and with KPI reporting on the `KPIs/metrics` tab: ready for Upper Management consumption.


<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

_This project requires following softwares, packages and tools._

  1. **Python 3.9**
  2. **AWS** to host various components of the **DVD_rental** ELT Pipeline application.
  3. **Databricks** to execute the transformation and testing steps of the pipeline.
  4. **PowerBI** to load and present the semantic reporting layer.

### Setup

_Below are the steps for setting up and running the dvd_rental ELT process._

1. Install Airbyte localy
2. Set up AWS account for RDS use
3. Set up Databricks account
4. Install PowerBI

### Airbyte Integration

1. Execute Airbyte integration: RDS -> Databricks

### Databricks Transformations and Testing

1. Load notebooks and workflow to Databricks
2. Run workflow

### PowerBI - Semantic layer and reporting

1. Initialize PowerBI
2. Ensure connection parameters to Databricks host
3. Refresh data and view dashboard results


<!-- USAGE EXAMPLES -->
## Usage

An ELT data pipeline solution is essential for collecting, transforming, and loading data from various sources into a centralized repository. The pipeline benefits BI analysts and Upper management. It centralizes the data for easy accessibility, standardizes and ensures data quality, consistency, and accuracy, and automates the process of data transformation and scaling up as the data volume grows. Consequently, data consumers can quickly access high-quality, consistent, and easily accessible data for making informed decisions (Upper management) and further business analytics (BI).


<!-- ROADMAP -->
## Roadmap

- [X] **`Data extraction:`**
    - [X] Extract data using either full extract
- [X] **`Data loadijng:`**
    - [X] Loading data to lakehouse tables using either full load
- [X] **`Data transformation:`**
    - [X] Transform data using big data technologies (i.e. Databricks).
    - [X] Use the following transformation techniques :  renaming columns, joining, grouping, typecasting, data filtering, sorting, and aggregating 
- [X] **`Data modelling:`**
    - [X] One fact table
    - [X] Three dimension tables
    - [X] One big table
- [X] **`Semantic modelling and visualization:`**
    - [X] Semantic dataset
    - [X] At least two semantic layer metrics
    - [X] At least two charts/visualizations
- [X] **`Data quality tests:`**
    - [X] At least five data quality tests (Great Expectations)
- [X] **`Dependencies between transformation tasks:`**
    - [X] Databricks Workflow: bronze -> silver -> gold
- [X] **`Git for collaboration`**
    - [X] Git commits
    - [X] Git pushes
    - [X] Git branching
    - [X] Git PRs/reviews
- [X] **`Deployment to Cloud`**
    - [X] Data integration: Airbyte
    - [X] Data transformation: Databricks
    - [X] Data lakehouse: Databricks
    - [X] Data Orchestration: Databricks Workflow
    

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!


<!-- CONTACT -->
## Contact


**Vasanth Nair** - [@Linkedin](https://www.linkedin.com/in/vasanthnair/) 

**Daniel Marinescu** - [@Linkedin](https://www.linkedin.com/in/danielmarinescu2/) 

**Project Link:** [https://github.com/vasanthnair/dvd_rental](https://github.com/vasanthnair/dvd_rental)

