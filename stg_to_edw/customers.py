import psycopg2
import sys
sys.path.append('C:/Users/kusumasri.muddasani/Desktop/ETL/EDW')
from etlbatch import etl_batch
#from datetime import datetime


etl_batch_no = etl_batch()[0]
etl_batch_date = etl_batch()[1]
# print(etl_batch_date)
#etl_batch_date = (datetime.strptime(etl_batch()[1], '%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')
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

        copy_command= (f'''INSERT INTO dev_edw.customers
(  src_customerNumber     ,
   customerName           ,
   contactLastName        ,
   contactFirstName       ,
   phone                  ,
   addressLine1           ,
   addressLine2            ,
   city                    ,
   state                   ,
   postalCode              ,
   country                ,
   salesRepEmployeeNumber  ,
   creditLimit             ,
   src_create_timestamp    ,
   src_update_timestamp ,
   etl_batch_no ,
   etl_batch_date  
)
SELECT
  c.CUSTOMERNUMBER
, c.CUSTOMERNAME
, c.CONTACTLASTNAME
, c.CONTACTFIRSTNAME
, c.PHONE
, c.ADDRESSLINE1
, c.ADDRESSLINE2
, c.CITY
, c.STATE
, c.POSTALCODE
, c.COUNTRY
, c.SALESREPEMPLOYEENUMBER
, c.CREDITLIMIT
, c.create_timestamp
, c.update_timestamp
, '{ETL_BATCH_NO}'
, '{ETL_BATCH_DATE}'
FROM dev_stg.customers c left join dev_edw.customers c1 on c.customerNumber=c1.src_customerNumber
left join dev_edw.employees e on c.salesrepemployeenumber=e.employeenumber
where c1.src_customerNumber is null;''')

        copy_command1=(f'''UPDATE dev_edw.customers a
        SET src_customerNumber     =b.CUSTOMERNUMBER,
        customerName           =b.CUSTOMERNAME,
        contactLastName        =b.CONTACTLASTNAME,
        contactFirstName       =b.CONTACTFIRSTNAME,
        phone                  =b.PHONE,
        addressLine1           =b.ADDRESSLINE1,
        addressLine2           =b.ADDRESSLINE2,
        city                   =b.CITY,
        state                  =b.STATE,
        postalCode             =b.POSTALCODE,
        country                =b.COUNTRY,
        salesRepEmployeeNumber =b.SALESREPEMPLOYEENUMBER,
        creditLimit            =b.CREDITLIMIT,
        src_update_timestamp   =b.update_timestamp,
        dw_update_timestamp    =CURRENT_TIMESTAMP,
        etl_batch_no           ='{ETL_BATCH_NO}',
        etl_batch_date         ='{ETL_BATCH_DATE}'
        from dev_stg.customers b
        where a.src_customerNumber=b.CUSTOMERNUMBER;
        ''')

        cursor.execute(copy_command)
        connection.commit()
        cursor.execute(copy_command1)
        connection.commit()

        print(f"Data uploaded to Redshift table customers")

    except Exception as e:
        print(f"Error uploading to Redshift customers: {e}")

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
