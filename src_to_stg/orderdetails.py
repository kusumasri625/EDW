import psycopg2
import boto3
from io import StringIO

# AWS S3 credentials
aws_access_key_id = 'AKIA4EXJQNRV27CLO4MM'
aws_secret_access_key = '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'
s3_bucket_name = 'classicmodeltos3'
s3_key = "orderdetails.csv"

# Redshift credentials
redshift_host = "default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_database = "dev"
redshift_user = "admin"
redshift_password = "BizAct#12345"
redshift_schema = "dev_stg"
redshift_table = "orderdetails"


def upload_to_redshift(host, port, database, user, password,schema, table):
    try:
        # Create a connection to Redshift
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        copy_command= ('''COPY dev.dev_stg.orderdetails FROM 's3://classicmodeltos3/orderdetails.csv' 
                        IAM_ROLE 'arn:aws:iam::834787109995:role/service-role/AmazonRedshift-CommandsAccessRole-20240220T172246' 
                        FORMAT AS CSV DELIMITER ',' QUOTE '"' ACCEPTINVCHARS '_' IGNOREHEADER 1 EMPTYASNULL 
                        REGION AS 'us-east-1';''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table: {schema}.{table}")

    except Exception as e:
        print(f"Error uploading to Redshift: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

s3=boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

upload_to_redshift(
    redshift_host,
    redshift_port,
    redshift_database,
    redshift_user,
    redshift_password,
    redshift_schema,
    redshift_table
)
