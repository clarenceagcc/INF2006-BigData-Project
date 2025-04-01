# INF2006-BigData-Project

## Processing the data
1. Clone this repo to get the .jar files required to process the dataset
   
     - YelpReviewDataset.jar
     - YelpTipBusinessCheckinDataset.jar
     - YelpUserDataset.jar
       
3. Download the Yelp JSON files: https://business.yelp.com/data/resources/open-dataset/
   
4. We'll be using the following for our analysis and visualization.
   
     - yelp_academic_dataset_business.json
     - yelp_academic_dataset_checkin.json
     - yelp_academic_dataset_review.json
     - yelp_academic_dataset_tip.json
     - yelp_academic_dataset_user.json
       
5. Upload the .jar and .json files to Amazon S3
   
![image](https://github.com/user-attachments/assets/ad1ee96d-c490-420a-9fa5-f3fac413f3aa)

6. Connect to an EMR with Hadoop and Spark
   
7. Create the input folder in the Hadoop HDFS and move the .json and .jar files to it
     ```
     # Example code
     hadoop fs -mkdir -p hdfs:///hadoop/input # we will put the files here
     hadoop fs -cp s3://your.uri.to.the.file.here/YelpUserDataset.jar hdfs:///hadoop/input # repeat for the all files were processing
     hadoop fs -ls hdfs:///hadoop/input/ # to preview files
     hadoop fs -copyToLocal hdfs:///user/hadoop/output/jarfile /home/hadoop/local_output/ # we need to move the .jar file to local to run it
     ```
8.  Now we can run the Hadoop .jar file to process our datasets and move it to S3 for long term storage
     ```
     # Example code
     hadoop jar /home/hadoop/YelpUserDataset.jar <input> <output> # replace input and output with the respective hdfs file locations
     # for now our output will be located at our HDFS file system, lets move it to our S3
     hadoop fs -ls hdfs:///hadoop/output/user_data_processed # example file output directory this will contain our part-r-000000
     # once we verified its there, we can move it
     hadoop fs -cp hdfs:///hadoop/output/user_data_processed s3://your.uri.to.the.output.folder.here/
     # we can verify it by refreshing the S3 page
     ```

## PySpark Analysis
After getting all of our Datasets processed, we need to do some filtering for usage in our streamlit website for visualization, we have our pyspark .py files that can be used to do analysis and filtering

1. Connect to EMR (make sure this has pyspark)
   
2. Find the PySpark analysis code in the repo (under /pyspark_analysis)

3. Create a new .py file to put our code in to it
     ```
     nano user_data_analysis.py # to create a new .py file
     # now just copy our code over, you will need to change the directories of the S3
     ```
4. Once all the directories point to the correct files/folders, we can run it with:
     ```
     spark-submit user_data_analysis.py
     ```
     This will take awhile since the dataset files are very big, but the output will be a few .csvs with the filtered columns and aggregated values to be used for visualization

## Running our Visualization
We are mainly leveraging on plotly to do our interactive plotting, so we are hosting it on Streamlit because its easy to create an interface to display everything

