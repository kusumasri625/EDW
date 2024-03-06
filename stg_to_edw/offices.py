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

        copy_command= (f'''INSERT INTO dev_edw.offices
( officeCode,
   city,
   phone,
   addressLine1          ,
   addressLine2          ,
   state                 ,
   country               ,
   postalCode            ,
   territory             ,
   src_create_timestamp  ,
   src_update_timestamp ,
   etl_batch_no,
   etl_batch_date   
)
SELECT
  A.OFFICECODE
, A.CITY
, A.PHONE
, A.ADDRESSLINE1
, A.ADDRESSLINE2
, A.STATE
, A.COUNTRY
, A.POSTALCODE
, A.TERRITORY
, A.create_timestamp
, A.update_timestamp
, '{ETL_BATCH_NO}'
, '{ETL_BATCH_DATE}'
FROM dev_stg.offices A left join dev_edw.offices B ON A.officeCode=B.officeCode
where B.officeCode IS NULL;''')

        copy_command1=(f'''
UPDATE dev_edw.offices a
set
officeCode               =b.OFFICECODE,
   city                  =b.CITY,
   phone                 =b.PHONE,
   addressLine1          =b.ADDRESSLINE1,
   addressLine2          =b.ADDRESSLINE2,
   state                 =b.STATE,
   country               =b.COUNTRY,
   postalCode            =b.POSTALCODE,
   territory             =b.TERRITORY,
   src_update_timestamp  =b.update_timestamp,
   dw_update_timestamp   =CURRENT_TIMESTAMP,
   etl_batch_no          ='{ETL_BATCH_NO}',
   etl_batch_date        ='{ETL_BATCH_DATE}'
   from dev_stg.offices b
where a.officeCode =b.OFFICECODE ''')

        cursor.execute(copy_command)
        connection.commit()
        cursor.execute(copy_command1)
        connection.commit()

        print(f"Data uploaded to Redshift table offices")

    except Exception as e:
        print(f"Error uploading to Redshift offices: {e}")

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
