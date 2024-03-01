import psycopg2
from io import StringIO

etl_batch_no=1001
etl_batch_date='2001-01-01'

def etl_batch():
    connection = psycopg2.connect(
    host="default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com",
    port="5439",
    database="dev",
    user="admin",
    password="BizAct#12345"
    )
    cursor = connection.cursor()

    query1 = f'update dev.etl_metadata.batch_control set etl_batch_no={etl_batch_no}, etl_batch_date=\'{etl_batch_date}\';'
    cursor.execute(query1)

    query= f'select etl_batch_date from dev.etl_metadata.batch_control;'
    cursor.execute(query)
    result=cursor.fetchone()

    # Extract the date string
    date_string = result[0].strftime('%Y-%m-%d') if result and result[0] else None

    connection.commit()

    cursor.close()
    connection.close()

    return date_string

etl_batch()
