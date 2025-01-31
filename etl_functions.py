def extract_data_from_wiki_page(URL):

    from bs4 import BeautifulSoup
    import pandas as pd
    import requests

    # 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity#Football_stadiums_by_capacity'
    
    res = requests.get(URL)

    soup = BeautifulSoup(res.text, 'html.parser')

    table = soup.find_all("table", class_= "wikitable sortable sticky-header")

    table_rows = table[0].find_all('tr')

    header = [col_name.getText().replace('\n', '') for col_name in table_rows[0].find_all("th")][1:]
    header[-1] = 'Home teams'

    data = [[val.getText()for val in row.find_all("td")] for row in table_rows[1:]]

    df = pd.DataFrame(data= data, columns= header)

    df['id'] = range(len(data))

    df = df[['id'] + header]

    df.to_csv('stads.csv', index= False)

    print('Successfully extracted stadiums data')


def transform_data(data_path):

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf, col
    from pyspark.sql.types import StringType
    import glob
    import os

    spark = SparkSession.builder.appName('stads').config("spark.executor.memory", "2g").config("spark.driver.memory", "2g").getOrCreate()
    
    output_dir = "output"

    def clean_text(text):
        
	    import re
	    
	    # replace newline characters with an empty string
	    if text:
	    	text = text.replace('\n', '')
	    
	    # replace the '♦' character with an empty string
	    if text:
	    	text = text.replace('♦', '')
	    
	    # remove leading and trailing whitespace
	    if text:
	    	text = re.sub(r'^\s+|\s+$', '', text)
	    
	    return text


    def clean_integer(string):
        
    	import re

    	string = re.sub(r'\[.*?\]', '', string)
        
    	if string:
        
        	string = string.replace(',', '') 

    	return string


    df = spark.read.csv(data_path, inferSchema= True, header= True, multiLine= True)

    # create udfs

    clean_text_udf = udf(clean_text, StringType())

    clean_integer_udf = udf(clean_integer, StringType())

    # apply transformations to the df

    # for all columns except Images and id, clean text
    for column in df.columns:

        if (column != 'Images') and (column != 'id'):

            df = df.withColumn(column, clean_text_udf(df[column]))

        # for Seating capacity column, apply integer specific transformations
        df = df.withColumn('Seating capacity', clean_integer_udf(df['Seating capacity']))


    # convert Seating capacity to integer
    
    df = df.withColumn("Seating capacity", col("Seating capacity").cast('integer'))

    df.write.csv(output_dir, header= True, mode= 'overwrite')
    
    spark.stop()

    print('Successfully transformed and exported stadiums data')

    csv_files = glob.glob('output/*.csv')

    # Ensure there's exactly one CSV file
    
    print(csv_files)
    
    if len(csv_files) == 1:

        temp_csv_path = csv_files[0]
     
        # Rename the file using os.rename
        os.rename(temp_csv_path, 'output/transformed_data.csv')

        print('output succefully renamed')

    else:
        print('found more or less than 1 file')


def load_into_Bq(project_id, dataset_id, table_id, data_path):

    from google.cloud import bigquery

    # initialize BigQuery client
    client = bigquery.Client(project= project_id)

    # construct the reference to the table
    table_ref = client.dataset(dataset_id).table(table_id)

    # load the data from the CSV file into the BigQuery table
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Skip header row

    job_config.autodetect = True  # Automatically detect schema

    with open(data_path, 'rb') as source_file:

        job = client.load_table_from_file(source_file, table_ref, job_config= job_config)

    # wait for the job to complete
    job.result()

    print(f'Loaded {job.output_rows} rows into {dataset_id}.{table_id}')


