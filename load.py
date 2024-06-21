from google.cloud import bigquery

project_id = 'world-stadiums-427116'
dataset_id = 'world_stadiums'
table_id = 'stadiums'

# initialize BigQuery client
client = bigquery.Client(project= project_id)

# convert the dictionary to a DataFrame
#df = pd.DataFrame(data)

# construct the reference to the table
table_ref = client.dataset(dataset_id).table(table_id)

# write the DataFrame to a temporary CSV file
csv_file_path = '/home/marwen/Applications/experiments/stadiums_data_pipeline/output/stadiums_transformed.csv' 
#df.to_csv(csv_file_path, index= False)

# load the data from the CSV file into the BigQuery table
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1  # Skip header row

job_config.autodetect = True  # Automatically detect schema

with open(csv_file_path, 'rb') as source_file:

    job = client.load_table_from_file(source_file, table_ref, job_config= job_config)

# wait for the job to complete
job.result()

print(f'Loaded {job.output_rows} rows into {dataset_id}.{table_id}')