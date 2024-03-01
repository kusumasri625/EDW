import oracledb
import sys
import csv
import boto3
from io import StringIO

ETL_BATCH_DATE = sys.argv[1]

sql_query = f"""select * from employees@f23kusumasri_dblink_classicmodels where to_char(update_timestamp,'YYYY-MM-DD')>='${ETL_BATCH_DATE}'"""

# AWS S3 credentials
aws_access_key_id = 'AKIA4EXJQNRV27CLO4MM'
aws_secret_access_key = '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'
s3_bucket_name = 'classicmodeltos3'
s3_key = "employees.csv"

def export_to_csv(query, csv_path):
    try:
        # Connect to the Oracle database
        connection = oracledb.connect('tpc1gb/tpc1gb@3.234.208.164:1521/XE')
        cursor = connection.cursor()

        # Execute the SQL query
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()

        # Get column names
        column_names = [col[0] for col in cursor.description]
        # Write data to CSV file
        with open(csv_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            
            # Write header
            csv_writer.writerow(column_names)

            # Write data
            csv_writer.writerows(rows)

        print(f"Data exported to {csv_path}")
        return csv_path

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

def upload_to_s3(csv_path, access_key_id, secret_access_key, bucket_name, s3_key):
    try:
        # Create an S3 client
        s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

        # Upload the CSV file to S3
        with open(csv_path, 'rb') as data:
            s3.upload_fileobj(data, bucket_name, s3_key)

        print(f"CSV file uploaded to S3 bucket: {bucket_name}/{s3_key}")

    except Exception as e:
        print(f"Error uploading to S3: {e}")

# Call the functions with your credentials and SQL query
csv_file_path = export_to_csv(sql_query, "employees.csv")

if csv_file_path:
    upload_to_s3(csv_file_path, aws_access_key_id, aws_secret_access_key, s3_bucket_name, s3_key)
