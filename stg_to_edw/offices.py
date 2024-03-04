import psycopg2

# Redshift credentials
redshift_host = "default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_database = "dev"
redshift_user = "admin"
redshift_password = "BizAct#12345"


def connect_to_redshift(host, port, database, user, password):
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

        copy_command= ('''INSERT INTO dev_edw.offices
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
, '${ETL_BATCH_NO}'
, '${ETL_BATCH_DATE}'
FROM dev_stg.offices A left join dev_edw.offices B ON A.officeCode=B.officeCode
where B.officeCode IS NULL;


UPDATE dev_edw.offices a,dev_stg.offices b
set
a.officeCode               =b.OFFICECODE,
   a.city                  =b.CITY,
   a.phone                 =b.PHONE,
   a.addressLine1          =b.ADDRESSLINE1,
   a.addressLine2          =b.ADDRESSLINE2,
   a.state                 =b.STATE,
   a.country               =b.COUNTRY,
   a.postalCode            =b.POSTALCODE,
   a.territory             =b.TERRITORY,
   a.src_update_timestamp  =b.update_timestamp,
   a.dw_update_timestamp   =CURRENT_TIMESTAMP,
   a.etl_batch_no          ='${ETL_BATCH_NO}',
   a.etl_batch_date        ='${ETL_BATCH_DATE}'
where a.officeCode =b.OFFICECODE ''')

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
    redshift_password
)
