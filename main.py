import os
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from flask import Flask, render_template, request
from google.cloud import bigquery

app = Flask(__name__)

# Global counters
rows_read = 0
rows_written = 0

@app.route('/', methods=['GET', 'POST'])
def main():
    global rows_read, rows_written

    if request.method == 'POST':
        # The button was clicked, trigger processing
        # Reset counters before processing
        rows_read = 0
        rows_written = 0

        # Your existing code for data processing...

    # Access environment variables
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    table_name = os.environ.get('TABLE_NAME')

    url = "https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp"
    response = requests.get(url)

    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", class_="display localtable")

    headers = [th.text.strip() for th in table.find_all("th")]
    headers = [element.replace(" ","_") for element in headers]

    rows = []
    for tr in table.find_all("tr"):
        row = [td.text.strip() for td in tr.find_all("td")]
        if row:
            rows.append(row)
            rows_read += 1

    df = pd.DataFrame(rows, columns=headers)
    df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], format="%d %b %y").dt.strftime("%Y-%m-%d")
    current_timestamp = datetime.now()
    df.loc[:, "Timestamp"] = current_timestamp

    output_file = 'edited_boe_rate.csv'
    df.to_csv(output_file, index=False)
    rows_written += len(df)

    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True

    with open(output_file, 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()

    # Render HTML template with results and counters
    return render_template('results.html', results=df.to_html(), timestamp=current_timestamp,
                           rows_read=rows_read, rows_written=rows_written)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
