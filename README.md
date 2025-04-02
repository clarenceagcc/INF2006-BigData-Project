# INF2006-BigData-Project: Yelp Data Analysis and Visualization

This project analyzes and visualizes the Yelp Open Dataset to gain insights into user reviews, business information, and other related data. It utilizes Apache Hadoop and Spark for data processing and filtering, and Streamlit with Plotly for interactive visualizations.

Here is the site hosted : https://inf2006-bigdata-project.streamlit.app/

## Project Overview

The workflow consists of three main stages:

1.  **Data Processing:** Using Hadoop and Spark to process the large Yelp JSON files and generate intermediate datasets.
2.  **PySpark Analysis (Data Filtering):** Employing PySpark to further filter and aggregate the processed data into CSV files suitable for visualization.
3.  **Visualization:** Creating an interactive web application with Streamlit and Plotly to display the analysis results.

## Prerequisites

Before you begin, ensure you have the following:

* **Amazon Web Services (AWS) Account:** Required for using Amazon S3 and Amazon EMR.
* **AWS CLI Configured:** To interact with AWS services from the command line.
* **Hadoop and Spark Knowledge:** Basic understanding of distributed computing concepts.
* **Python 3.x:** For running PySpark analysis and the Streamlit application.
* **Java Development Kit (JDK):** Required for running Hadoop and Spark.

## 1. Data Processing (Hadoop)

This stage involves processing the raw Yelp JSON data using Hadoop and pre-built JAR files.

###   1.1. Required Files

* **JAR Files:** Obtain these from this repository.
    * `YelpReviewDataset.jar`
    * `YelpTipBusinessCheckinDataset.jar`
    * `YelpUserDataset.jar`
* **Yelp JSON Datasets:** Download from the official Yelp website: [https://www.yelp.com/dataset](https://www.yelp.com/dataset)
    * `yelp_academic_dataset_business.json`
    * `yelp_academic_dataset_checkin.json`
    * `yelp_academic_dataset_review.json`
    * `yelp_academic_dataset_tip.json`
    * `yelp_academic_dataset_user.json`

###   1.2. Setup on AWS

1.  **Upload to Amazon S3:**
    * Create an S3 bucket to store your data and JAR files.
    * Upload the JAR files and the Yelp JSON datasets to this S3 bucket.

2.  **Launch Amazon EMR Cluster:**
    * Launch an EMR cluster with Hadoop and Spark installed.
    * Configure the cluster with sufficient resources to handle the dataset size.

###   1.3. HDFS Interaction

1.  **Create Input Directory:** Connect to the EMR master node and create an input directory in HDFS:

    ```bash
    hadoop fs -mkdir -p hdfs:///hadoop/input
    ```

2.  **Copy Files to HDFS:** Copy the JAR files and JSON datasets from S3 to the HDFS input directory. Replace `<s3://your.uri.to.the.file.here/>` with the correct S3 path.

    ```bash
    hadoop fs -cp s3://your.uri.to.the.file.here/YelpUserDataset.jar hdfs:///hadoop/input
    hadoop fs -cp s3://your.uri.to.the.file.here/yelp_academic_dataset_business.json hdfs:///hadoop/input
    # Repeat the 'hadoop fs -cp' command for all JAR and JSON files
    ```

3.  **List Files in HDFS (Verification):**

    ```bash
    hadoop fs -ls hdfs:///hadoop/input/
    ```

4.  **Copy JAR File to Local (EMR):** Copy the JAR file from HDFS to the local filesystem on the EMR master node. This is necessary to execute the JAR.

    ```bash
    hadoop fs -copyToLocal hdfs:///hadoop/input/YelpUserDataset.jar /home/hadoop/local_output/YelpUserDataset.jar
    # Repeat for other JAR files if needed
    ```

###   1.4. Run Hadoop JAR Jobs

1.  **Execute JAR:** Run the Hadoop JAR file to process the datasets. Replace `<input>` and `<output>` with the corresponding HDFS paths.

    ```bash
    hadoop jar /home/hadoop/local_output/YelpUserDataset.jar hdfs:///hadoop/input hdfs:///hadoop/output/user_data_processed
    # Repeat the 'hadoop jar' command for each JAR file, adjusting input/output paths and JAR name
    ```

2.  **List Output in HDFS (Verification):**

    ```bash
    hadoop fs -ls hdfs:///hadoop/output/user_data_processed
    # Modify the path as needed to match your output directory names
    ```

3.  **Copy Output to S3:** Copy the processed data from HDFS to your S3 bucket for storage.

    ```bash
    hadoop fs -cp hdfs:///hadoop/output/user_data_processed s3://your.uri.to.the.output.folder.here/user_data_processed
    # Adjust the paths to match your output directories and S3 bucket
    ```

4.  **Verify in S3:** Confirm that the processed data is successfully copied to your S3 bucket.

## 2. PySpark Analysis (Data Filtering)

This stage uses PySpark to filter and aggregate the processed data from S3, generating CSV files for visualization.

###   2.1. EMR Setup (with PySpark)

1.  **Launch EMR Cluster (with PySpark):** Ensure that your EMR cluster has PySpark installed. You might need to select a different instance type or add Spark during cluster creation.

###   2.2. PySpark Code

1.  **Locate PySpark Scripts:** Find the PySpark analysis scripts in the `/pyspark_analysis` directory of this repository.

2.  **Create PySpark Script:** Connect to the EMR master node and create a new Python file (e.g., `user_data_analysis.py`) to hold your PySpark code.

    ```bash
    nano user_data_analysis.py
    ```

3.  **Copy Code:** Copy the contents of the relevant PySpark script into the new file.

4.  **Modify S3 Paths:** **Crucially, update the S3 paths within the PySpark script to point to your input and output locations.** This is the most common source of errors.

###   2.3. Run PySpark Script

1.  **Execute PySpark:** Submit the PySpark script to the Spark cluster:

    ```bash
    spark-submit user_data_analysis.py
    ```

2.  **Monitor Execution:** The script may take a significant amount of time to run, depending on the dataset size and cluster resources. Monitor the Spark application logs for progress and errors.

3.  **Verify Output:** After successful execution, verify that the generated CSV files are present in your specified output location (likely an S3 bucket).

## 3. Visualization (Streamlit)

This stage uses Streamlit and Plotly to create interactive visualizations from the processed CSV files.

###   3.1. Dependencies

1.  **Install Python Requirements:** Navigate to the directory containing the Streamlit application and install the required Python libraries.

    ```bash
    pip install -r requirements.txt
    ```

    * If you encounter any missing library errors during the next step, install them individually using:

        ```bash
        pip install <missing_library_name>
        ```

###   3.2. Run Streamlit Application

1.  **Execute Streamlit:** Run the Streamlit application using the following command (assuming your main Streamlit script is named `Home.py`):

    ```bash
    streamlit run Home.py
    ```

2.  **Access Application:** Streamlit will provide a local URL in the terminal. Open this URL in your web browser to view the interactive visualizations.

## Troubleshooting

* **S3 Path Errors:** Double-check all S3 paths in your PySpark scripts and Hadoop commands.
* **EMR Cluster Configuration:** Ensure your EMR cluster has sufficient resources and the necessary software (Hadoop, Spark, PySpark).
* **JAR File Execution:** Verify that the JAR files are correctly compiled and that you have the correct Java version.
* **Python Dependencies:** Use a virtual environment to manage Python dependencies and ensure all required libraries are installed.
