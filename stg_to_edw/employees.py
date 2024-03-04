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


def connect_to_redshift(host, port, database, user, password,etl_batch_no,etl_batch_date):
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

        copy_command= ('''INSERT INTO dev_edw.employees
(employeeNumber            ,
   lastName               ,
   firstName              ,
   extension              ,
   email                  ,
   officeCode             ,
   reportsTo              ,
   jobTitle               ,
   dw_office_id           ,
   src_create_timestamp   ,
   src_update_timestamp   ,
   etl_batch_no,
   etl_batch_date  
)
SELECT
  e.EMPLOYEENUMBER
, e.LASTNAME
, e.FIRSTNAME
, e.EXTENSION
, e.EMAIL
, e.OFFICECODE
, e.REPORTSTO
, e.JOBTITLE
, o.officecode
, e.create_timestamp
, e.update_timestamp
,'${ETL_BATCH_NO}'
,'${ETL_BATCH_DATE}'
FROM dev_stg.employees e left join dev_edw.employees e1 on e.employeeNumber=e1.employeeNumber
 inner join dev_edw.offices o on e.officecode=o.officecode
where e1.employeeNumber is null;



UPDATE f23kusumasri_devdw.employees a,f23kusumasri_devdw.employees b
SET a.dw_reporting_employee_id=b.dw_employee_id
where a.reportsTo=b.employeeNumber;


UPDATE f23kusumasri_devdw.employees a,f23kusumasri_devstage.employees b
SET 
a.employeeNumber            =b.EMPLOYEENUMBER,
   a.lastName               =b.LASTNAME,
   a.firstName              =b.FIRSTNAME,
   a.extension              =b.EXTENSION,
   a.email                  =b.EMAIL,
   a.officeCode             =b.OFFICECODE,
   a.reportsTo              =b.REPORTSTO,
   a.jobTitle               =b.JOBTITLE,
   a.src_update_timestamp   =b.update_timestamp,
   a.dw_update_timestamp    =CURRENT_TIMESTAMP,
   a.etl_batch_no           ='${ETL_BATCH_NO}',
   a.etl_batch_date         = '${ETL_BATCH_DATE}'
where a.employeeNumber =b.employeeNumber;

''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table")

    except Exception as e:
        print(f"Error uploading to Redshift: {e}")

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
