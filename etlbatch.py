import psycopg2
from io import StringIO

etl_batch_no=1001
etl_batch_date='2001-01-01'
schema_name = 'cm_20050609'
identified = 'cm_20050609123'

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

    query= f'select * from dev.etl_metadata.batch_control;'
    cursor.execute(query)
    result=cursor.fetchone()

    date_string=[]
    date_string.append(result[0])
    # Extract the date string
    s = result[1].strftime('%Y-%m-%d') if result and result[1] else None
    date_string.append(s)
    
    date_string.append(schema_name)
    
    date_string.append(identified)
    
    connection.commit()

    cursor.close()
    connection.close()

    return date_string

etl_batch()
