import os
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from flask import Flask
from google.cloud import bigquery

app = Flask(__name__)

@app.route('/')
def main():
    # Access environment variables
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    table_name = os.environ.get('TABLE_NAME')

    # Your existing code here

    url = "https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp"
    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table with class "display localtable"
    table = soup.find("table", class_="display localtable")

    # Extract the table headers
    headers = [th.text.strip() for th in table.find_all("th")]
    headers = [element.replace(" ","_") for element in headers]

    # Extract the table rows
    rows = []
    for tr in table.find_all("tr"):
        row = [td.text.strip() for td in tr.find_all("td")]
        if row:
            rows.append(row)

    # Create a DataFrame from the table data
    df = pd.DataFrame(rows, columns=headers)

    # Convert the date column values to the desired format
    df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], format="%d %b %y").dt.strftime("%Y-%m-%d")

    # Add a new column 'timestamp' with timestamps
    df.loc[:, "Timestamp"] = datetime.now()

    # Save the edited DataFrame to a new CSV file
    output_file = 'edited_boe_rate.csv'
    df.to_csv(output_file, index=False)

    # Load the CSV file to BigQuery table
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True  # Infer table schema from CSV file

    with open(output_file, 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Wait for the job to complete

    return f'Successfully loaded {job.output_rows} rows into {project_id}.{dataset_id}.{table_name}'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
