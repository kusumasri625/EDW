import psycopg2
import sys
sys.path.append('C:/Users/kusumasri.muddasani/Desktop/ETL/EDW')
from etlbatch import etl_batch


etl_batch_no = etl_batch()[0]
etl_batch_date = etl_batch()[1]
# Redshift credentials
redshift_host = "default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_database = "dev"
redshift_user = "admin"
redshift_password = "BizAct#12345"


def connect_to_redshift(host, port, database, user, password,ETL_BATCH_NO,ETL_BATCH_DATE):
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

        copy_command= (f'''update dev_edw.productlines a
set
productLine            =b.productline,
   src_update_timestamp=b.update_timestamp,
   dw_update_timestamp =CURRENT_TIMESTAMP,
   etl_batch_no        ='{ETL_BATCH_NO}',
   etl_batch_date     ='{ETL_BATCH_DATE}'
   from dev_stg.productlines b
where a.productline=b.productline;''')

        copy_command1=(f'''
insert into dev_edw.productlines
(
productLine           ,
   src_create_timestamp,
   src_update_timestamp,
   etl_batch_no        ,
   etl_batch_date        
)
SELECT
  p.PRODUCTLINE
, p.create_timestamp
, p.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
FROM dev_stg.productlines p left join dev_edw.productlines p1 on p.productLine=p1.productLine
where p1.productLine is null;''')

        cursor.execute(copy_command1)
        connection.commit()
        cursor.execute(copy_command)
        connection.commit()

        print(f"Data uploaded to Redshift table productlines")

    except Exception as e:
        print(f"Error uploading to Redshift productlines: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

connect_to_redshift(
    redshift_host,
    redshift_port,
    redshift_database,
    redshift_user,
    redshift_password,
    etl_batch_no,
    etl_batch_date
)
