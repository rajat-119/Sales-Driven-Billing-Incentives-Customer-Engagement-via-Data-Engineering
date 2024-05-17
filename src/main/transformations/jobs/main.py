import shutil
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.s3_client_object import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session
from src.main.transformations.jobs import dimension_tables_join
from src.main.write.parquet_writer import ParquetWriter

aws_access_key = config.aws_access_key

aws_secret_key = config.aws_secret_key


s3_client_provider = S3ClientProvider(decrypt(aws_access_key),decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
print(response)
logger.info('List of Buckets: %s',response['Buckets'])

# write comments

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f""" select distinct file_name from 
                de_project.product_staging_table 
                where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A' 
                """
    logger.info(f"dynamically statement created:{statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info('Your last run was failed please check')
    else:
        logger.info('No record match')
else:
    logger.info('Last run was successful!!!!')

try:
    s3_reader = S3Reader()

    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path = folder_path)
    logger.info('Absolute path on s3 bucket csv file %s', s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception('No Data available to process')
except Exception as e:
    logger.error('Exited with error:- %s', e)
    raise e
#['s3://de-rajat-project-1/sales_data/heart.csv', 's3://de-rajat-project-1/sales_data/kidney.csv', 's3://de-rajat-project-1/sales_data/liver.csv']


prefix = f"s3://{config.bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info(f"File path available on s3 under %s bucket and folder name is %s",config.bucket_name,file_paths)
try:
    downloader = S3FileDownloader(s3_client,config.bucket_name,config.local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error('File download error : %s',e)

#To check the list of files downloaded from above code
all_files = os.listdir(config.local_directory)
logger.info(f"list of files present at my local directory after download {all_files}")

# Checking only for csv files and creating absolute paths
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith('.csv'):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(config.local_directory,files)))
    if not csv_files:
        logger.error('No csv data available to process the request')
        raise Exception("No csv data available to process the request ")
else:
    logger.error('There is no data to process.')
    raise Exception('There is no data to process.')

logger.info("List of csv files that needs to be precessed %s",csv_files)

logger.info('*************************Creating spark session******************')

spark = spark_session()

logger.info('*************************Spark session created *******************')


# Checking the required column in the schema of csv files
#if the column does not match...we will keep the file in the error_file

logger.info('***************** checking Schema for the data/file loaded *********************')


correct_files = []
for data in csv_files:
    data_schema = spark.read.format('csv') \
                .option('header','true') \
                .load(data).columns
    logger.info(f'Schema for the {data} is {data_schema}')
    missing_column =  set(data_schema) - set(config.mandatory_columns)
    logger.info(f'Missing columns are {missing_column}')

    if missing_column:
        error_files.append(data)
    else:
        logger.info(f'No missing column for {data}')
        correct_files.append(data)

logger.info(f'************* List of correct files ***************{correct_files}')
logger.info(f'************* List of error files ***************{error_files}')
logger.info(f'************* Moving the error data/files to error directory in S3 **************')



error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            # ***** moving error file to error file directory in local
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)
            shutil.move(file_path, destination_path)# move file from one directory to another
            logger.info(f"In local moved{file_name}from files from s3 folder to {destination_path}")


            # ***** moving error file to error file directory in local s3
            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix,file_name)
            logger.info(f'{message}')
        else:
            logger.info(f'{file_path} does not exist.')
else:
    logger.info('********** There is no error files available at our dataset ************')


# extra and additional column needs to be taken care
# Before running the process Stage table needs to be updated with as Active(A) or Inactive(I)
logger.info(f'********** Updating the product_staging_table that we have started the process ************')
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_Date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        statements = f""" Insert into {db_name}.{config.product_staging_table}
                          (file_name, file_location, created_date, status)
                           VALUES ('{file_name}','{file}','{formatted_Date}','A')"""
        insert_statements.append(statements)

    logger.info(f'Insert statement created for staging able---{insert_statements}')
    logger.info('*************Connecting with My SQL server *************')
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logging.info('**********My SQL Connected successfully ***********')
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info('************ There is no file to process ************')
    raise Exception('***No Data Available with correct files ******')

logger.info('*********** Staging table updated successfully ************')
logger.info('*********** Fixing extra column from source ***********')

schema = StructType([StructField('customer_id', IntegerType(),True),
                    StructField('store_id', IntegerType(),True),
                    StructField('product_name', StringType(),True),
                    StructField('sales_date', StringType(),True),
                    StructField('sales_person_id', IntegerType(),True),
                    StructField('price', FloatType(),True),
                    StructField('Quantity', IntegerType(),True),
                    StructField('Total_cost', FloatType(),True),
                    StructField('additional_column', StringType(),True)])

database_client = DatabaseReader(config.url, config.properties)
logger.info('********* creating empty dataframe ***********')
final_df_to_process = database_client.create_dataframe(spark,'empty_df_create_table')

#final_df_to_process = spark.createDataFrame([],schema = schema)
final_df_to_process.show()
for data in correct_files:
    data_df = spark.read.format('csv') \
                .option('header','true') \
                .option('inferSchema','true') \
                .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f'Extra columns present at source is {extra_columns}')
    if extra_columns:
        data_df = data_df.withColumn('additional_column',concat_ws(",",*extra_columns)) \
                    .select('customer_id','store_id','product_name','sales_date','sales_person_id',
                            'price','quantity','total_cost','additional_column')
        logger.info(f'processed {data} and added additional_column')
    else:
        # if the dataframe does not have any additional column but still the final_df_to_process data_frame has a column by name additional_column so we have to pass None in this now
        data_df = data_df.withColumn('additional_column',lit(None)) \
                    .select('customer_id','store_id','product_name','sales_date','sales_person_id',
                            'price','quantity','total_cost','additional_column')

    final_df_to_process = final_df_to_process.union(data_df)

logger.info('******** Final Dataframe from source which will be going to processing*******')
final_df_to_process.show()

#Enriching the data from all dimention table( meaning connecting the final_df_to_process dataframe with customer, sales and other tables)

#Connection with DatabaseReader
database_client = DatabaseReader(config.url,config.properties)
#Creating dataframe for all the tables (customer,sales, etc.) present in sql_script folder

#Customer table
logger.info('*********** Loading customer table into customer_table_df *************')
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)

#product_table
logger.info('*********** Loading product table into product_table_df *************')
product_table_df = database_client.create_dataframe(spark,config.product_table)

#product_staging_table
logger.info('*********** Loading staging table into product_staging_table_df *************')
product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)

#sales_team_table
logger.info('************ Loading sales_team_table into sales_team_table_df *************')
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)

#store_table
logger.info('********** Loading store table into store_table_df ***********')
store_table_df = database_client.create_dataframe(spark,config.store_table)

#enriching the data from different table:- joining the final_df_to_process, customer_table_df, store_table_df,sales_team_table_df
s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process, customer_table_df,store_table_df,sales_team_table_df)

logger.info('******* Final Enriched Data ***********')
s3_customer_store_sales_df_join.show()

#writing the customer data into customer data mart in parquet format

logger.info('*********** write the data into customer data mart ***********')
final_customer_data_mart_df = s3_customer_store_sales_df_join \
                                .select('ct.customer_id','ct.first_name','ct.last_name','st.address',
                                        'ct.pincode','phone_number','sales_date','total_cost')
logger.info(f'************* Final Data for customer Data Mart ************')
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter('overwrite','csv')
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)
logger.info(f'*********** customer data written to local disk at {config.customer_data_mart_local_file}*****')

#moving data on s3 bucket for customer_data_mart
logger.info(f'********* Data Movement from local to s3 for customer data mart *****************')
s3_uploader = UploadToS3(s3_client)# creating instance
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f'{message}')

logger.info(" **************** Write the data into sales team Data Mart ****************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join \
                                .select('store_id','sales_person_id','sales_person_first_name',
                                        'sales_person_last_name','store_manager_name','manager_id','is_manager',
                                        'sales_person_address','sales_person_pincode','sales_date','total_cost',
                                        expr('SUBSTRING(sales_date,1,7) as sales_month'))

logger.info('***********Final data fro sales team data mart ***********')
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)

logger.info(f'**********Sales team data written to local disk at {config.sales_team_data_mart_partitioned_local_file}************************')


#Moving data on se for sales_data_mart
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.sales_team_data_mart_local_file)

logger.info(f'{message}')

#Writing the sales_team_data_mart into partitions
final_sales_team_data_mart_df.write.format('parquet')\
                        .option('header','true')\
                        .mode('overwrite')\
                        .partitionBy('sales_month','store_id')\
                        .option('path',config.sales_team_data_mart_partitioned_local_file)\
                        .save()

#Moving data on s3 for partitioned folder
s3_prefix = 'sales_partitioned_data_mart'
current_epoch = datetime.date.today()
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root,file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f'{s3_prefix}/{current_epoch}/{relative_file_path}'
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)


#calculating for customer mart:-
    #find out the customer total purchase every month
    #write the data into MySql table

logger.info('************ Calculating customer every month purchased amount ***********')
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info('*********** Calculation of customer mart done and written into the table**************')


#calculating for sales team mart:-
    #find out the total sales done by each sales person every month
    #Give the top performer 1% incentive of total slaes of the month
    #Rest sales person will get nothing
    #write the data into MySql table

logger.info('************Calculating sales month billed amount ***********')

sales_mart_calculation_table_write(final_sales_team_data_mart_df)

logger.info('************Calculation of sales mart done and written into the table*********')

#Moving the file on s3 into processed folder and delete the local files

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory

message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(message)

logger.info('************Deleting sales data from local ************')
delete_local_file(config.local_directory)
logger.info('********** Deleted sales data from local')

logger.info('************Deleting customer_data_mart from local ************')
delete_local_file(config.customer_data_mart_local_file)
logger.info('********** Deleted customer_data_mart data from local')

logger.info('************Deleting sales_team_data_mart data from local ************')
delete_local_file(config.sales_team_data_mart_local_file)
logger.info('********** Deleted sales_team_data_mart data from local')

logger.info('************Deleting sales_team_data_mart_partitioned data from local ************')
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info('********** Deleted sales_team_data_mart_partitioned data from local')

# Because the process is completed fully..so we need to change the status of the stagging table to "I" i.e.(inactive)
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f""" UPDATE {db_name}.{config.product_staging_table}
                    SET status = 'I',updated_date = '{formatted_Date}'
                    WHERE file_name = '{filename}' """
        update_statements.append(statements)

    logger.info('***********Update Statement created for staging table *****')
    logger.info('************ Connecting with MySql server *************')
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info('******** MySql server connected successful **************')
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info('********* There is some error in process in between ********')
    sys.exit()
input('Press enter to terminate')