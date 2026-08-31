import sys
import json
import ast
import re
from datetime import datetime
from awsglue.transforms import ApplyMapping, SelectFields, ResolveChoice, DropNullFields
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark import StorageLevel
from pyspark.sql.functions import col
import boto3
from reusable_functions_sfmgn import err_log_entry
from connect import get_postgres_vw, get_postgres_vw_partn, get_secret, audit_sumry,pg_insert, restart_job, restart_job_pipeline, PrintException

# Capturing start time of the job
v_start_time = datetime.utcnow()
start_time=v_start_time.strftime("%Y-%m-%d %H:%M:%S")
# print(start_time)


args = getResolvedOptions(sys.argv, ['JOB_NAME','region','secret_name','param_file_name',\
'config_bucket_name','ods_cat_db_name','ods_aud_cat_tb_name','config_subdir',\
'job_stat_completion','job_stat_failure','sns_arn','run_env','cert_file_name','stage_bucket_name',\
'delimiter','ods_table_name','file_type','envision_param_file_name','trade_event_src_sys','trade_time_src_sys'])

# Initialize global variables
src_cnt, src_fltr_cnt, rej_cnt, fltr_cnt, tgt_cnt = 0,0,0,0,0

# Define dynamicframe and spark dataframe
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

CONF_JOB_NAME = args['JOB_NAME']
CONF_REGION = args['region']
CONF_TABLE_NM = args['ods_table_name']
CONF_FILE_TYPE = args['file_type']
SSC_CONF_PARAM_FILE_NM = args['param_file_name']
ENV_CONF_PARAM_FILE_NM= args['envision_param_file_name']
CONF_SUBDIR = args['config_subdir']
CONF_STG_BKT_NM = args['stage_bucket_name']
CONF_BUCKETNAME = args['config_bucket_name']
CONF_ODS_CAT_DB_NAME = args['ods_cat_db_name']
CONF_ODS_AUD_CAT_TB_NAME = args['ods_aud_cat_tb_name']
CONF_CERT_FILE = args['cert_file_name']
CONF_FAIL_STAT = args['job_stat_failure']
SNS_ARN= args['sns_arn']
RUN_ENV = args['run_env']
CONF_RUN_STAT = args['job_stat_completion']
CONF_FILE_DELIMITER = args['delimiter']
if CONF_FILE_TYPE == 'event_trade':
    CONF_ERR_LOG_SRC_SYS = args['trade_event_src_sys']
elif CONF_FILE_TYPE == 'time_trade':
    CONF_ERR_LOG_SRC_SYS = args['trade_time_src_sys']
elif CONF_FILE_TYPE == 'maintenance':
    CONF_ERR_LOG_SRC_SYS = args['trade_time_src_sys']
job = Job(glueContext)
job.init(CONF_JOB_NAME, args)

# Read JSON Parameter file
s3 = boto3.resource('s3')

# CONTENT_OBJECT = s3.Object(CONF_BUCKETNAME, str(CONF_SUBDIR) + str(CONF_PARAM_FILE_NM))
# FILE_CONTENT = CONTENT_OBJECT.get()['Body'].read().decode('utf-8')

# Define DB Parameters
CONF_SECRET_NM = args['secret_name']
SECRET = get_secret(CONF_SECRET_NM,CONF_REGION)
DBNAME = SECRET['dbname']
DBPORT = SECRET['port']
DBUSER = SECRET['username']
DBPWD = SECRET['password']
DBHOST = SECRET['host']
DB_URL = "jdbc:postgresql://"+str(DBHOST)+":"+str(DBPORT)+"/"+str(DBNAME)

# if CONF_TYPE == 0:
#     pass
# elif CONF_TYPE == 1:
#     CONF_TRUNCATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('truncate_query')
#     if CONF_TRUNCATE_QUERY:
#         TRUNCATE_QUERY = CONF_TRUNCATE_QUERY.format(CONF_SCHEMA)
#     CONF_IDENTIFR_QUERY = JSON_CONTENT[CONF_TABLE_NM]['identifier_query']
#     CONF_UPDATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('update_query')
# else :
#     CONF_IDENTIFR_QUERY = JSON_CONTENT[CONF_TABLE_NM]['identifier_query']
#    # CONF_TGT_SUR_ID_COL = JSON_CONTENT[CONF_TABLE_NM]['tgt_sur_id_col']
#     CONF_SCD_2_UPDATE = JSON_CONTENT[CONF_TABLE_NM]['scd_2_update']
#     SCD_2_UPDATE_QUERY = CONF_SCD_2_UPDATE.format(CONF_SCHEMA,'2025-05-08' , CONF_JOB_NAME) #CONF_BATCH_DATE

# Function to send SNS notification for missing transaction codes
def send_trx_alert(record_count, missing_codes):
    try:
        print("In send_trx_alert function try block")
        sns_client = boto3.client('sns', region_name='us-east-1')
       
        SUB = f"Action Required - PMF New Transaction Codes in {RUN_ENV}"
       
        MSG = f"{record_count} new transaction code/codes (trx_cd) have been received that are not present in the trx_mastr table.\n\n"
        MSG += f"Transaction Code/Codes: {missing_codes}\n\n"
        MSG += "These transactions have been loaded with Transaction Master ID = '-99'.\n"
        MSG += "Please query the 'abc_err_log' table to review the newly received trx_cd values and update the Transaction Master accordingly.\n"
       
        response = sns_client.publish(
            TopicArn = SNS_ARN,
            Message = f"{MSG}",
            Subject = f"{SUB}"
        )
       
        print(f"SNS Response: {response}")
        return True
       
    except Exception as e:
        print("Exception in sending sns notification: ", e)
        raise e
       
def send_acct_typ_alert(record_count, missing_codes):
    try:
        print("In send_acct_typ_alert function try block")
        sns_client = boto3.client('sns', region_name='us-east-1')

        SUB = f"Action Required - PMF New Account Type Codes in {RUN_ENV}"

        MSG = f"{record_count} new account type code/codes (acct_typ_cd) have been received that are not present in the ACCT_TYP_MSTR table.\n\n"
        MSG += f"Account Type Code/Codes: {missing_codes}\n\n"
        MSG += "These records have NOT been loaded and require review.\n"
        MSG += "Please query the 'abc_err_log' table to review the newly received acct_typ_cd values and update the Account Type Master accordingly.\n"

        response = sns_client.publish(
            TopicArn=SNS_ARN,
            Message=f"{MSG}",
            Subject=f"{SUB}"
        )
        print(f"SNS Response: {response}")
        return True

    except Exception as e:
        print("Exception in sending sns notification: ", e)
        raise e

def send_acct_typ_dq_alert(record_count, reason_summary):
    try:
        print("In send_acct_typ_dq_alert function try block")
        sns_client = boto3.client('sns', region_name='us-east-1')

        SUB = f"Action Required - PMF Account Type Data Quality Failures in {RUN_ENV}"

        MSG = f"{record_count} record(s) failed account type data quality validation and were NOT loaded.\n\n"
        MSG += f"Failure Reason Breakdown:\n{reason_summary}\n\n"
        MSG += "Please query the 'abc_err_log' table to review the flagged records and correct source data accordingly.\n"

        response = sns_client.publish(
            TopicArn=SNS_ARN,
            Message=f"{MSG}",
            Subject=f"{SUB}"
        )
        print(f"SNS Response: {response}")
        return True

    except Exception as e:
        print("Exception in sending sns notification: ", e)
        raise e      
 
# Function to read file from S3
def read_from_s3(bucket_name, file_key):
    """Read file content from an S3 bucket."""
    try:
        s3_client = boto3.client('s3')
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_obj['Body'].read().decode('utf-8')
        print(f"{file_key} read successfully")
        return file_content
    except Exception as e:
        print(f"Error reading param {file_key} from S3: {e}")
        audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
        CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
       src_cnt, tgt_cnt, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
        print("Audit entry successful!")
        sys.exit(e)



# Reading source file
def read_source_file(CONF_SRC_FILE_PATH,CONF_FILE_DELIMITER):
    try:
        global src_cnt,src_fltr_cnt
        if CONF_TGT_TBL_NM != 'plcy_trx_sumry':
            print(f"Reading {CONF_SRC_FILE_PATH} delimete {CONF_FILE_DELIMITER}")
            src_df = glueContext.read.format('csv').option("delimiter", CONF_FILE_DELIMITER).option("quote", "\"").option("escape", "\"").options(header = 'true').load(CONF_SRC_FILE_PATH)
            src_df.persist(StorageLevel.MEMORY_AND_DISK)
            src_df.createTempView("src_file_vw")
            src_df.show(20)
            src_cnt = src_df.count()
            print("Source data as read from file with count:", src_cnt)
        else:
            conf_src_query = CONF_SRC_QUERY.format(CONF_SCHEMA,CONF_BATCH_ID)
            print("conf_src_query: ",conf_src_query)
            src_df = get_postgres_vw(glueContext,conf_src_query, DB_URL, DBUSER, DBPWD)
            src_df.persist(StorageLevel.MEMORY_AND_DISK)
            src_df.show(20)
            src_cnt = src_df.count()
            print("Source data as read from file with count:", src_cnt)
        # Applying filter condition on source df if applicable
        if CONF_SRC_FLTR_QUERY and CONF_TGT_TBL_NM != 'plcy_trx_sumry':
            if CONF_TGT_TBL_NM == 'invstmt_fund_trx' or CONF_TGT_TBL_NM =='invstmt_fund_asset':
                conf_src_fltr_query = CONF_SRC_FLTR_QUERY.format(CONF_BATCH_DATE)
            else:
                conf_src_fltr_query = CONF_SRC_FLTR_QUERY.format(CONF_SCHEMA)
            print("conf_src_fltr_query: ",conf_src_fltr_query)
            src_df = spark.sql(conf_src_fltr_query)
            src_df.persist(StorageLevel.MEMORY_AND_DISK)
            src_df.show(20)
            src_fltr_df_cnt = src_df.count()
            print("After running src_filter query,source data count:", src_fltr_df_cnt)
            if CONF_MLTI_TGT_FLAG:
                src_cnt = src_fltr_df_cnt
            src_fltr_cnt = src_cnt - src_fltr_df_cnt
            print("Total records filtered from source:", src_fltr_cnt)
            src_df.createTempView("src_vw")
        if CONF_TGT_TBL_NM == 'plcy_trx_sumry':
            conf_src_fltr_query = CONF_SRC_FLTR_QUERY.format(CONF_SCHEMA,CONF_BATCH_ID)
            print("conf_src_fltr_query: ",conf_src_fltr_query)
            src_df = get_postgres_vw(glueContext,conf_src_fltr_query, DB_URL, DBUSER, DBPWD)
            src_df.persist(StorageLevel.MEMORY_AND_DISK)
            src_df.show(20)
            src_fltr_df_cnt = src_df.count()
            print("After running src_filter query,source data count:", src_fltr_df_cnt)
            src_df.createTempView("src_vw")

        return src_cnt, src_fltr_cnt
    except Exception as e:
        print(f"Error reading source file from S3: {e}")
        audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
        CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
        src_cnt, tgt_cnt, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
        print("Audit entry successful!")
        sys.exit(e)


def src_tgt_mapping(ods_cat_db_name,ods_aud_cat_tb_name,job_name,tgt_file_nm,src_file_nm,CONF_BATCH_ID,src_sys,src_df,catalog_db,catalog_table):
  try:
    string_schema=str(src_df.schema())
    string_schema = string_schema.split('[')[1]    

    clean_list = list(map(lambda x: x.replace("Field","").replace("StructType","") \
                .replace("({})","").replace("{}","").replace("({}","") \
                .replace("("," ").replace(", "," "),string_schema.split('),')))
    input_list=[x.strip() for x in clean_list  if "Type" in x ]
    output_list=[]
    for i in range(len(input_list)):
        separator=str(input_list[i]).split(' ')
        separator[1]=separator[1].split('Type')
        if(separator[1][0]=="Decimal"):
            output_list.append(str(separator[0])+'*'+ str(separator[1][0]+'('+ separator[2]+','+separator[3]+')').lower())
        else:
            output_list.append(str(separator[0])+'*'+ str(separator[1][0]).lower())

    src_list=list(map(lambda x: x.capitalize(),output_list))        
    src_list.sort()
    print(f"Source List: {src_list}and len:{len(src_list)}")

    glue=boto3.client('glue')
    catalog_list=[]
    response = glue.get_table(DatabaseName=catalog_db,Name=catalog_table)
    src_col_nm = [col.split('*')[0].upper() for col in src_list]

    for i in response['Table']['StorageDescriptor']['Columns']:
      if i['Name'].upper() in src_col_nm:
        catalog_list.append(i['Name']+'*'+i['Type'])
    catalog_list=list(map(lambda x: x.capitalize(),catalog_list))
    catalog_list.sort()
    print(f"Catalog List {catalog_list}and len:{len(catalog_list)}")

    for i in range(len(src_list)):
        src_list[i]=str(src_list[i])+'*'+str(catalog_list[i])
    temp=[]
    for i in src_list:
        temp.append(i.split('*'))
    final_map=list(map(tuple,temp))
    return final_map
  except Exception as err:
    print(f"Failed to create apply map: {err}")
    audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
    CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
    src_cnt, tgt_cnt, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
    print("Audit entry successful!")
    sys.exit(err)


def stg_file_to_table_load(job_name,tgt_file_nm,src_file_nm,CONF_BATCH_ID,tgt_load_df,src_sys,ods_cat_db_name,ods_aud_cat_tb_name,ods_cat_tgt_tb_nm):
    try:
        tgt_count = tgt_load_df.count()
        if tgt_count !=0:
            print("Records for new insert: ",tgt_count)
            #Convert df to Glue dynamic frame
            src_dyn_frm = DynamicFrame.fromDF(tgt_load_df, glueContext, "src_dyn_frm")

            map_fields =src_tgt_mapping(ods_cat_db_name,ods_aud_cat_tb_name,job_name,tgt_file_nm,src_file_nm,CONF_BATCH_ID,src_sys,src_dyn_frm,ods_cat_db_name,ods_cat_tgt_tb_nm)
            map_fields = [tuple(item.upper() if ind in (0,2) else item for ind,item in enumerate(my_tuple) ) for my_tuple in map_fields]
            print(f"map_fields:{map_fields}")

            ## Apply map operation so Glue will understand its input and output mapping
            final_applymapping = ApplyMapping.apply(frame = src_dyn_frm, mappings = map_fields, transformation_ctx = "final_applymapping")
            print('Apply mapping is completed!')

            final_resolvechoice = ResolveChoice.apply(frame = final_applymapping, \
            choice = "MATCH_CATALOG", database = ods_cat_db_name, table_name = ods_cat_tgt_tb_nm, \
            transformation_ctx = "final_resolvechoice")

            final_resolvechoice1 = ResolveChoice.apply(frame = final_resolvechoice, \
            choice = "make_cols", transformation_ctx = "final_resolvechoice1")

            dyf_dropNullfields = DropNullFields.apply(frame = final_resolvechoice1)

            datasink = glueContext.write_dynamic_frame.from_catalog(frame = dyf_dropNullfields, \
            database = ods_cat_db_name, table_name = ods_cat_tgt_tb_nm,  \
            transformation_ctx = "datasink")
            print("Target table loaded successfully!")
    except Exception as e:
            print(f"Unable to load table {ods_cat_tgt_tb_nm}: {e}")
            audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
    CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
    src_cnt, 0, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
            print("Audit entry successful!")
            sys.exit(e)



def load_ods_table(load_type=None):
    try:
        global src_cnt,src_fltr_cnt,fltr_cnt,rej_cnt,tgt_cnt
        print(f'load_type:{load_type}')
        if (CONF_TGT_TBL_NM =='invstmt_fund_trx' or CONF_TGT_TBL_NM =='invstmt_fund_asset') and load_type!='second_load':
           print(CONF_DELETE_QUERY)
           pg_insert(DBUSER,DBHOST,DBPORT,DBNAME,DBPWD,CONF_DELETE_QUERY,CONF_CERT_FILE,CONF_BUCKETNAME, CONF_REGION)
           print(f"current batch records are deleted for {CONF_TGT_TBL_NM.upper()}")
        # Reading current target table data
        CURR_TBL_QUERY = CONF_CURR_TBL_QUERY.format(CONF_SCHEMA)
        curr_df = get_postgres_vw_partn(glueContext,CURR_TBL_QUERY,DB_URL,DBUSER,DBPWD,CONF_SUR_KEY,None,None,None)
        print(f" Current target table {CONF_TGT_TBL_NM} read successfully ")
        curr_df.createOrReplaceTempView('curr_vw')
        curr_tbl_count = curr_df.count()
        print("curr_tbl_count: ", curr_tbl_count)
        curr_df.show(5)

        # Reading Required Tables
        if CONF_LKP_TABLES and load_type !='second_load':
            print("----- Reading required tables for lookup! -----")
            for i in range(int(CONF_LKP_TABLES)) :
                TABLE = 'table_'+str(i+1)
                if CONF_TGT_TBL_NM == 'plcy_trx_sumry':
                    CONF_TBL_QUERY = (JSON_CONTENT[CONF_TABLE_NM][TABLE]).format(CONF_SCHEMA,CONF_BATCH_DATE,CONF_BATCH_ID)
                else:
                    CONF_TBL_QUERY = (JSON_CONTENT[CONF_TABLE_NM][TABLE]).format(CONF_SCHEMA)
                table_df = get_postgres_vw(glueContext,CONF_TBL_QUERY,DB_URL,DBUSER,DBPWD)
                table_df.createOrReplaceTempView(TABLE+'_vw')
                #table_df.persist(StorageLevel.MEMORY_AND_DISK)
                table_df.show(5)
                lkp_tbl_cnt = table_df.count()
                #unpersist_dfs.append(table_df)
                print(f"View Created for {TABLE}, total count from ref table: {lkp_tbl_cnt}")


        # Generating intermediate table views
        if CONF_TEMP_TABLES and load_type !='second_load':
            print("----- Generating intermediate temp tables for lookup! -----")

            for i in range(int(CONF_TEMP_TABLES)) :
                TABLE = 'temp_table_'+str(i+1)
                CONF_TBL_QUERY = (JSON_CONTENT[CONF_TABLE_NM][TABLE])
                temp_table_df = spark.sql(CONF_TBL_QUERY)
                temp_table_df.createOrReplaceTempView(TABLE+'_vw')
                #temp_table_df.persist(StorageLevel.MEMORY_AND_DISK)
                temp_table_df.show(10)
                temp_tbl_cnt = temp_table_df.count()
                #unpersist_dfs.append(temp_table_df)
                if CONF_TGT_TBL_NM == 'invstmt_fund_trx' and i+1 == 2:
                    print(f"CONF_SRC_FILE_NAME: {CONF_SRC_FILE_NAME}")
                    if CONF_ERR_LOG_SRC_SYS == 'SSC_EVENT_ENV_TRADE':
                        print(f"Processing Error Logs for system: {CONF_ERR_LOG_SRC_SYS}")
                        CONF_ABC_ERR_LOG_QUERY_TRADE_EC25 = CONF_SRC_FILE_KEY= JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_25', CONF_ABC_ERR_LOG_QUERY_TRADE_EC25,CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD,tgt_tbl_nm=CONF_TGT_TBL_NM)

                        CONF_ABC_ERR_LOG_QUERY_OMNI_EC25 = CONF_SRC_FILE_KEY= JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_25', CONF_ABC_ERR_LOG_QUERY_OMNI_EC25,CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD,tgt_tbl_nm=CONF_TGT_TBL_NM)
 
 
                        CONF_ABC_ERR_LOG_QUERY_TRADE = CONF_SRC_FILE_KEY= JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_trade']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_26', CONF_ABC_ERR_LOG_QUERY_TRADE,CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD,tgt_tbl_nm=CONF_TGT_TBL_NM)
 
                        CONF_ABC_ERR_LOG_QUERY_OMNI = CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_omni']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_26', CONF_ABC_ERR_LOG_QUERY_OMNI,CONF_ERR_LOG_SRC_SYS,CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD,tgt_tbl_nm=CONF_TGT_TBL_NM)
                    else:
                        print(f"Processing Error Logs for system: {CONF_ERR_LOG_SRC_SYS}")
                        CONF_ABC_ERR_LOG_QUERY = CONF_SRC_FILE_KEY= JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_25', CONF_ABC_ERR_LOG_QUERY,CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD)
 
                        CONF_ABC_ERR_LOG_QUERY_TIMEBASED = CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_timebased']
           
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_26',CONF_ABC_ERR_LOG_QUERY_TIMEBASED, CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME, CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD)
           
                    print("Initiating check for missing trx_cd against master table for SNS Alert...")
                    try:
                        CONF_MISSING_TRX_ALERT_QUERY = JSON_CONTENT[CONF_TABLE_NM]['missing_trx_alert_query']
               
                        missing_trx_df = spark.sql(CONF_MISSING_TRX_ALERT_QUERY)
                        missing_count = missing_trx_df.count()
               
                        if missing_count > 0:
                            missing_rows = missing_trx_df.select("TRX_CD").distinct().collect()
                            codes_list = [str(row['TRX_CD']) for row in missing_rows]
                            codes_string = ", ".join(codes_list)
                   
                            print(f"Found {missing_count} missing transaction codes. Triggering SNS alert...")
                   
                            send_trx_alert(missing_count, codes_string)
                        else:
                            print("No missing transaction codes found. No alert needed.")
                   
                    except Exception as e:
                        print(f"Exception occurred while checking missing trx_cd or sending alert: {e}")
       
                print(f"View Created for {TABLE}, total count from temp table: {temp_tbl_cnt}")

        print(f"DEBUG: CONF_TGT_TBL_NM={CONF_TGT_TBL_NM}, CONF_TABLE_NM={CONF_TABLE_NM}, CONF_TEMP_TABLES={CONF_TEMP_TABLES}")
        print(f"CONF_FILE_TYPE: {CONF_FILE_TYPE}")      
        if CONF_TGT_TBL_NM == 'pty_detl':
                print(f"CONF_SRC_FILE_NAME: {CONF_SRC_FILE_NAME}")

                # ---------- New ACCT_TYP_CD ----------
                print("Initiating check for missing acct_typ_cd against master table for SNS Alert...")
                try:
                    CONF_MISSING_ACCT_TYP_ALERT_QUERY = JSON_CONTENT[CONF_TABLE_NM]['missing_acct_typ_alert_query']
                    missing_acct_typ_df = spark.sql(CONF_MISSING_ACCT_TYP_ALERT_QUERY)
                    missing_acct_typ_count = missing_acct_typ_df.count()

                    if missing_acct_typ_count > 0:
                        missing_rows = missing_acct_typ_df.select("ACCT_TYP_CD").distinct().collect()
                        codes_list = [str(row['ACCT_TYP_CD']) for row in missing_rows]
                        codes_string = ", ".join(codes_list)

                        print(f"Found {missing_acct_typ_count} unexpected account type codes. Triggering SNS alert...")
                        send_acct_typ_alert(missing_acct_typ_count, codes_string)

                        # Log to error table (rule 6's own err_log query)
                        CONF_ABC_ERR_LOG_QUERY_ACCT_TYP = JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_acct_typ']
                        err_log_entry(spark, glueContext, missing_acct_typ_df, 'EC_27',
                                      CONF_ABC_ERR_LOG_QUERY_ACCT_TYP, CONF_ERR_LOG_SRC_SYS,
                                      CONF_SRC_FILE_NAME, CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME,
                                      DB_URL, DBUSER, DBPWD, tgt_tbl_nm=CONF_TGT_TBL_NM)
                   
                    else:
                          print("No unexpected account type codes found. No alert needed.")
                except Exception as e:
                       print(f"Exception occurred while checking missing acct_typ_cd or sending alert: {e}")

                print("Initiating account type data quality validation...")
                acct_typ_bad_keys_df = None
                try:
                    CONF_ACCT_TYP_DQ_QUERY = JSON_CONTENT[CONF_TABLE_NM]['acct_typ_dq_query']
                    acct_typ_dq_df = spark.sql(CONF_ACCT_TYP_DQ_QUERY)
                    acct_typ_dq_df.persist(StorageLevel.MEMORY_AND_DISK)
                    acct_typ_dq_count = acct_typ_dq_df.count()

                    if acct_typ_dq_count > 0:
                        reason_rows = acct_typ_dq_df.groupBy("DQ_REASON_CD").count().collect()
                        reason_summary = "\n".join(
                                [f"{row['DQ_REASON_CD']}: {row['count']} record(s)" for row in reason_rows]
                            )
                        print(f"Found {acct_typ_dq_count} records failing account type validation. Triggering SNS alert...")
                        send_acct_typ_dq_alert(acct_typ_dq_count, reason_summary)

                        acct_typ_dq_df.createOrReplaceTempView("temp_dq_vw")
                        CONF_ABC_ERR_LOG_QUERY_ACCT_TYP_DQ = JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_acct_typ_dq']
                        err_log_entry(spark, glueContext, acct_typ_dq_df, 'EC_28',
                                    CONF_ABC_ERR_LOG_QUERY_ACCT_TYP_DQ, CONF_ERR_LOG_SRC_SYS,
                                    CONF_SRC_FILE_NAME, CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME,
                                    DB_URL, DBUSER, DBPWD, tgt_tbl_nm=CONF_TGT_TBL_NM)

                        acct_typ_bad_keys_df = acct_typ_dq_df.select("pty_uniq_id").distinct()

                    else:
                        print("No account type data quality issues found. No records excluded.")

                    acct_typ_dq_df.unpersist()
                except Exception as e:
                        print(f"Exception occurred while validating acct_typ_cd or sending alert: {e}")      


        # droping duplicates if required based on provided columns
        if load_type !='second_load':
            # Executing lookup query to fetch the lookup column values
            print("----- Executing Lookup Query! -----")
            if CONF_TGT_TBL_NM =='invstmt_fund_trx' or CONF_TGT_TBL_NM =='invstmt_fund_asset' :
                CONF_LKP_QUERY = JSON_CONTENT[CONF_TABLE_NM]['lkp_query'].format(CONF_BATCH_DATE)
            else:
                CONF_LKP_QUERY = JSON_CONTENT[CONF_TABLE_NM]['lkp_query']
            stage_transformed_df1 = spark.sql(CONF_LKP_QUERY)
            #stage_transformed_df1.createTempView("lkp_vw")
            #stage_transformed_df1.persist(StorageLevel.MEMORY_AND_DISK)
            stage_transformed_df1.show(20)
            lkp_cnt = stage_transformed_df1.count()
            print(f"After lookup, count {lkp_cnt}")
            if CONF_TGT_TBL_NK :
                drop_dup_on_cols = CONF_TGT_TBL_NK.split(',')
                stage_transformed_df1 = stage_transformed_df1.dropDuplicates(drop_dup_on_cols)
            stage_transformed_df1.persist(StorageLevel.MEMORY_AND_DISK)
            stdf1_cnt = stage_transformed_df1.count()
            unpersist_lkp_dfs.append(stage_transformed_df1)
            stage_transformed_df1.show()
            stage_transformed_df1.createOrReplaceTempView("lkp_vw")

            dup_cnt = src_cnt - src_fltr_cnt - stdf1_cnt
            print('Total Records Dropped after duplicate check:', dup_cnt)
   
                # proceding only if records are present after rejecting
            if stdf1_cnt == 0 :
                tgt_cnt = 0
                print("Data count zero. Nothing to load !!!")
            else :
                if CONF_TYPE in (1,2):
                    #  Calculating Hashkey on src side. Also on tgt side in case of type 1
                    identifier_df = spark.sql(CONF_IDENTIFR_QUERY)
                   
                else :
                    identifier_df = stage_transformed_df1

                identifier_df.persist(StorageLevel.MEMORY_AND_DISK)
                #identifier_cnt = identifier_df.count()
                unpersist_lkp_dfs.append(identifier_df)
                #print(f'identifier_df count:{identifier_cnt}')
                # print("identifier_df:")
                identifier_df.show(10)

                # filtering out data
                identifier_df.createTempView("idntifr_df_vw")  
                REC_FLTR_QUERY = CONF_REC_AFTER_FLTR.format(CONF_BATCH_ID,CONF_JOB_NAME,CONF_BATCH_DATE)
                rec_after_fltr_df = spark.sql(REC_FLTR_QUERY)
                #rec_after_fltr_df.persist(StorageLevel.MEMORY_AND_DISK)
                #unpersist_lkp_dfs.append(rec_after_fltr_df)
                rec_after_fltr_df.createTempView("rec_after_fltr_df_vw")
                #rec_after_fltr_cnt = rec_after_fltr_df.count()
                #print(f"rec_after_fltr_df count:{rec_after_fltr_cnt}")
                print(f"rec_after_fltr_df")
                rec_after_fltr_df.show(10)
                if CONF_FILE_TYPE.lower() == 'full' and CONF_TYPE==2:
                    CONF_FULL_FILE_INACTIVE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('full_file_inactive_qry')
                    CONF_FULL_FILE_INACTIVE_QUERY = CONF_FULL_FILE_INACTIVE_QUERY.format(CONF_BATCH_ID,CONF_JOB_NAME,CONF_BATCH_DATE)
                    full_file_inactive_df = spark.sql(CONF_FULL_FILE_INACTIVE_QUERY)
                    print(f'CONF_FULL_FILE_INACTIVE_QUERY:{CONF_FULL_FILE_INACTIVE_QUERY}')
                    full_file_inactive_df.show()
                    rec_after_fltr_df = rec_after_fltr_df.union(full_file_inactive_df)

                #rec after flter cnt , idntfir cnt prints are removed

                # separating insert and update records in case of type 1 and creating final dataframe
                if CONF_TYPE == 1 and (CONF_TGT_TBL_NM != 'invstmt_fund_trx' and CONF_TGT_TBL_NM != 'invstmt_fund_asset'):
                     temp_sink_df = rec_after_fltr_df.where(col('INSERT_UPDATE_FLG') == 'U')
                     temp_sink_df.persist(StorageLevel.MEMORY_AND_DISK)
     
                     # Checking if there are any updates and calling map and table load if updates exist
                     if temp_sink_df.rdd.isEmpty() :
                         upd_tgt_cnt = 0
                         print("Nothing to Update...")
                     else :
                         upd_tgt_cnt = temp_sink_df.count()
                         print(f"{upd_tgt_cnt} Updates detected. Performing mapping and table load operation")
                         temp_sink_df.createOrReplaceTempView("tgt_sink_df_vw")
                         SELECT_QUERY = CONF_SELECT_QUERY.format(CONF_BATCH_ID,CONF_JOB_NAME,CONF_BATCH_DATE)
                         tmp_tgt_load_df = spark.sql(SELECT_QUERY)
                         stg_file_to_table_load(CONF_JOB_NAME,CONF_SRC_FILE_NAME,CONF_SRC_FILE_NAME,CONF_BATCH_ID,tmp_tgt_load_df,CONF_SRC_SYS,CONF_ODS_CAT_DB_NAME,CONF_ODS_AUD_CAT_TB_NAME,CONF_TEMP_UPD_TABLE)
                         pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, CONF_UPDATE_QUERY, CONF_CERT_FILE,CONF_BUCKETNAME, CONF_REGION)
                     tgt_sink_df = rec_after_fltr_df.where(col('INSERT_UPDATE_FLG') == 'I')
                     tgt_sink_df.show(5)
                     ins_tgt_cnt = tgt_sink_df.count()
                     tgt_cnt=ins_tgt_cnt+upd_tgt_cnt
           
                else:
                    tgt_sink_df = rec_after_fltr_df
                    if CONF_TGT_TBL_NM == 'invstmt_fund_asset' and load_type != 'second_load':
                        print("HOLD DEBUG - action codes:", [r[0] for r in tgt_sink_df.select('SC_ASST_POS_ACTN_CD').collect()])
                        tgt_sink_df = tgt_sink_df.where(col('SC_ASST_POS_ACTN_CD') != 'U')
                        print("HOLD DEBUG - rows kept after hold:", tgt_sink_df.count())
                    tgt_sink_df.persist(StorageLevel.MEMORY_AND_DISK)
                    tgt_sink_df.show(5)
                    tgt_cnt = tgt_sink_df.count()
 
        if CONF_TGT_TBL_NM == 'invstmt_fund_trx':
            if load_type == 'second_load':
                stdf1_cnt=0
                print("second time func")
                latest_tgt_rec_negate_df = spark.sql(CONF_LATEST_TGT_REC_NEGATE_QUERY)
                latest_tgt_rec_negate_df.persist(StorageLevel.MEMORY_AND_DISK)
                latest_tgt_rec_negate_cnt = latest_tgt_rec_negate_df.count()
                print(f'Reversal Qualified record count from target table:{latest_tgt_rec_negate_cnt}')
                latest_tgt_rec_negate_df.createOrReplaceTempView("LATEST_TGT_REC_NEGATE_VW")
                for df in unpersist_lkp_dfs:
                    df.unpersist()
                negate_cancel_upd_df = spark.sql(CONF_NEGATE_CANCEL_UPD_QUERY)
                negate_cancel_upd_cnt = negate_cancel_upd_df.count()
                print(f'Records for reversal:{negate_cancel_upd_cnt}')
                latest_tgt_rec_negate_df.unpersist()
                negate_cancel_upd_df.persist(StorageLevel.MEMORY_AND_DISK)
                negate_cancel_upd_df.createOrReplaceTempView("negate_cancel_upd_vw")
                tgt_sink_df = spark.sql(CONF_CANCEL_UPD_REVERSAL_QUERY)
                if CONF_TGT_TBL_NK :
                    drop_dup_on_cols = CONF_TGT_TBL_NK.split(',')
                    print("Duplicate check columns:",drop_dup_on_cols)
                    tgt_sink_df = tgt_sink_df.dropDuplicates(drop_dup_on_cols)
                tgt_sink_df.show()
                negate_cancel_upd_df.unpersist()
                tgt_sink_df.persist(StorageLevel.MEMORY_AND_DISK)
                tgt_cnt = tgt_sink_df.count()
                dup_cnt = negate_cancel_upd_cnt - tgt_cnt
                print('Total Records Dropped after duplicate check:', dup_cnt)
                # Checking if there are any inserts and calling map and table load if inserts exist
   

        # NEW — asset second_load in-place update
        if CONF_TGT_TBL_NM == 'invstmt_fund_asset' and load_type == 'second_load':
            stdf1_cnt = 0
            latest_tgt_rec_negate_cnt = 0
            arc_cnt = 0
            print("asset second load - in-place update against persisted rebook")
            upd_match_df = spark.sql(CONF_SECOND_LOAD_UPD_MATCH)
            upd_match_df.persist(StorageLevel.MEMORY_AND_DISK)
            upd_tgt_cnt = upd_match_df.count()
            print(f'asset U records matched to current rebook: {upd_tgt_cnt}')
            upd_match_df.show(5)
            if upd_tgt_cnt == 0:
                print("Nothing to Update in asset second load...")
            else:
                upd_match_df.createOrReplaceTempView("tgt_sink_df_vw")
                SELECT_QUERY = CONF_SELECT_QUERY.format(CONF_BATCH_ID, CONF_JOB_NAME, CONF_BATCH_DATE)
                tmp_tgt_load_df = spark.sql(SELECT_QUERY)
                tmp_tgt_load_df.show(5)
                stg_file_to_table_load(CONF_JOB_NAME, CONF_SRC_FILE_NAME, CONF_SRC_FILE_PATH, CONF_BATCH_ID, tmp_tgt_load_df, CONF_SRC_SYS, CONF_ODS_CAT_DB_NAME, CONF_ODS_AUD_CAT_TB_NAME, CONF_TEMP_UPD_TABLE)
                pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, CONF_UPDATE_QUERY, CONF_CERT_FILE, CONF_BUCKETNAME, CONF_REGION)
            upd_match_df.unpersist()
            tgt_cnt = 0
            # ---- NEW: unmatched held U -> insert as 'O' (spec 1a) ----
            upd_nomatch_df = spark.sql(CONF_SECOND_LOAD_UPD_NOMATCH)
            upd_nomatch_df.persist(StorageLevel.MEMORY_AND_DISK)
            nomatch_cnt = upd_nomatch_df.count()
            print(f"asset lone-U (no match) to insert as O: {nomatch_cnt}")
            if nomatch_cnt > 0:
                upd_nomatch_df.createOrReplaceTempView("tgt_sink_df_vw")
                INS_SELECT_QUERY = CONF_SELECT_QUERY.format(CONF_BATCH_ID, CONF_JOB_NAME)
                ins_load_df = spark.sql(INS_SELECT_QUERY)
                if CONF_TGT_TBL_NM == 'invstmt_fund_asset':
                    ins_load_df = ins_load_df.drop('temp_invstmt_fund_asset_id')
                stg_file_to_table_load(CONF_JOB_NAME, CONF_SRC_FILE_NAME, CONF_SRC_FILE_PATH,
                                    CONF_BATCH_ID, ins_load_df, CONF_SRC_SYS,
                                    CONF_ODS_CAT_DB_NAME, CONF_ODS_AUD_CAT_TB_NAME, CONF_CATALOG_TBL)
            upd_nomatch_df.unpersist()
           

        if tgt_cnt == 0 :
            print("Nothing to Insert...")
        else :
            print(f"{tgt_cnt} Inserts detected. Performing mapping and table load operation...")
            # Selecting required fields in tgt_sink_df
            tgt_sink_df.createOrReplaceTempView("tgt_sink_df_vw")                
            SELECT_QUERY = CONF_SELECT_QUERY.format(CONF_BATCH_ID, CONF_JOB_NAME)
            tgt_load_df = spark.sql(SELECT_QUERY)
            if CONF_TGT_TBL_NM == 'invstmt_fund_asset':                  # NEW
                tgt_load_df = tgt_load_df.drop('temp_invstmt_fund_asset_id')
            tgt_load_df.show(5)
            tgt_sink_df.unpersist()
            stg_file_to_table_load(CONF_JOB_NAME,CONF_SRC_FILE_NAME,CONF_SRC_FILE_NAME,CONF_BATCH_ID,tgt_load_df,CONF_SRC_SYS,CONF_ODS_CAT_DB_NAME,CONF_ODS_AUD_CAT_TB_NAME,CONF_CATALOG_TBL)
        if stdf1_cnt != 0:
            # Running SCD2 update in case of type 2 table
            if CONF_TYPE == 2 :
                #added if newly
                if CONF_TEMP_UPD_TABLE :
                    INS_TO_TEMP_TABLE_SQL = CONF_INS_TO_TEMP_TABLE_SQL.format(CONF_BATCH_ID,CONF_JOB_NAME)
                    tmp_load_df = spark.sql(INS_TO_TEMP_TABLE_SQL)
                    tmp_load_df.persist(StorageLevel.MEMORY_AND_DISK)
                    tmp_df_cnt = tmp_load_df.count()
                    unpersist_lkp_dfs.append(tmp_load_df)
                    if tmp_df_cnt != 0 :
                       stg_file_to_table_load(CONF_JOB_NAME,CONF_SRC_FILE_NAME,CONF_SRC_FILE_NAME,CONF_BATCH_ID,tmp_load_df,CONF_SRC_SYS,CONF_ODS_CAT_DB_NAME,CONF_ODS_AUD_CAT_TB_NAME,CONF_TEMP_UPD_TABLE)
                       print("Data Load to Temp Table completed ! ")
                    else:
                        print("Nothing to Insert to Temp Table !")
                pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, SCD_2_UPDATE_QUERY, CONF_CERT_FILE, CONF_BUCKETNAME, CONF_REGION)        
                if CONF_TGT_GTR_CNT_FLAG :
                    src_cnt = lkp_cnt
        # Getting Filtered and Rejected Counts
        if load_type =='second_load':
            fltr_cnt = latest_tgt_rec_negate_cnt - tgt_cnt
        else:
            fltr_cnt = src_cnt - tgt_cnt
        rej_cnt = src_cnt - fltr_cnt - tgt_cnt

        # Getting Target count
        CONF_TGT_CNT_QUERY = CONF_TGT_CNT_SQL.format(CONF_SCHEMA, CONF_BATCH_ID, CONF_JOB_NAME)
        final_count = get_postgres_vw(glueContext,CONF_TGT_CNT_QUERY, DB_URL, DBUSER, DBPWD)
        tgt_tbl_cnt = final_count.collect()[0][0]

        print("Source Count :", src_cnt)
        print("Filtered Count :", fltr_cnt)
        print("Rejected Count :", rej_cnt)
        print("Target Table Count :", tgt_tbl_cnt)
   
        return src_cnt, fltr_cnt, rej_cnt, tgt_tbl_cnt    
    except Exception as e:
        PrintException()
        #audit sumry entry
        audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
        CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
        src_cnt, 0, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
        print("Audit entry successful!")
        sys.exit(e)

try:
    print(f"Job started running at {start_time}")
    if 'asset' in CONF_FILE_TYPE or 'trade' in CONF_FILE_TYPE:
        CONF_PARAM_FILE_NM = ENV_CONF_PARAM_FILE_NM
    else:
        CONF_PARAM_FILE_NM = SSC_CONF_PARAM_FILE_NM
    FILE_CONTENT = read_from_s3(CONF_BUCKETNAME,str(CONF_SUBDIR) + str(CONF_PARAM_FILE_NM))
    JSON_CONTENT = json.loads(FILE_CONTENT)
    if CONF_FILE_TYPE.lower() == "maintenance":
        CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['maint_src_file_key']
        CONF_SRC_FILE_NAME = JSON_CONTENT[CONF_TABLE_NM]['maint_src_file_name']
        CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_timebased.json"
    elif CONF_FILE_TYPE.lower() == "full":
        CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['full_src_file_key']
        CONF_SRC_FILE_NAME = JSON_CONTENT[CONF_TABLE_NM]['full_src_file_name']
        CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_full_file.json"
    elif CONF_FILE_TYPE.lower() in ('event_trade','event_asset'):
        CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['event_src_file_key']
        CONF_SRC_FILE_NAME = JSON_CONTENT[CONF_TABLE_NM]['event_src_file_name']
        if CONF_FILE_TYPE.lower() == 'event_trade':
            CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_event_trade.json"
        else:
            CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_event_asset.json"
    elif CONF_FILE_TYPE.lower() in ('time_trade','time_asset'):
        CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['time_src_file_key']
        CONF_SRC_FILE_NAME = JSON_CONTENT[CONF_TABLE_NM]['time_src_file_name']
        CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_timebased.json"
    else:
        print("Please enter valid file type!")
        sys.exit("Please enter valid file type!")

    # Reading dynamic values from param gen
    config_param_lines = read_from_s3(CONF_BUCKETNAME,CONF_PARAM_DYNAMIC_FILE)
    if not config_param_lines:
        sys.exit(f"Error reading config file :{CONF_PARAM_DYNAMIC_FILE} from S3")
    config_param = json.loads(config_param_lines)
    CONF_BATCH_DATE = config_param['Dynamic']['batch_date']
    CONF_BATCH_ID = config_param['Dynamic']['btch_id']
   
    # Retriveing Param Information
    if  CONF_FILE_TYPE.lower() in ('maintenance') and 'customer' in CONF_TABLE_NM:
        CONF_SRC_SYS = JSON_CONTENT['Global']['Static']['customer_source_system']
    else:    
        CONF_SRC_SYS = JSON_CONTENT['Global']['Static']['source_system']
    CONF_SCHEMA = JSON_CONTENT['Global']['Static']['ods_schema_name']
    CONF_CATALOG_TBL = JSON_CONTENT[CONF_TABLE_NM]['catalog_tbl']
    CONF_SRC_FILE_PATH = "s3://" + str(CONF_STG_BKT_NM) + "/" + str(CONF_SRC_FILE_KEY) + str(CONF_SRC_FILE_NAME)
    print(f"Source file {CONF_SRC_FILE_NAME} in {CONF_SRC_FILE_KEY} path in {CONF_STG_BKT_NM}")
    CONF_TYPE = int(JSON_CONTENT[CONF_TABLE_NM]['table_load_type'])
    CONF_TEMP_UPD_TABLE = JSON_CONTENT[CONF_TABLE_NM].get('upd_temp_table_name')
    CONF_TGT_TBL_NM = JSON_CONTENT[CONF_TABLE_NM]['tgt_tbl_name']
    CONF_LKP_TABLES = JSON_CONTENT[CONF_TABLE_NM].get('lkp_tables')
    CONF_TEMP_TABLES = JSON_CONTENT[CONF_TABLE_NM].get('temp_tables')

    CONF_SRC_FLTR_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('src_fltr_query')
    CONF_SRC_FINAL_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('src_final_query')
    CONF_LKP_QUERY = JSON_CONTENT[CONF_TABLE_NM]['lkp_query']
    CONF_TGT_TBL_NK = JSON_CONTENT[CONF_TABLE_NM].get('tgt_tbl_nk')
    CONF_MLTI_TGT_FLAG = JSON_CONTENT[CONF_TABLE_NM].get('multi_tgt_flag')
    CONF_TGT_GTR_CNT_FLAG = JSON_CONTENT[CONF_TABLE_NM].get('tgt_gtr_cnt')
    CONF_CURR_TBL_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('cur_table')
    CONF_SUR_KEY = JSON_CONTENT[CONF_TABLE_NM].get('sur_key')
    CONF_TEMP_TABLE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('temp_table_query')
    CONF_REC_AFTER_FLTR = JSON_CONTENT[CONF_TABLE_NM].get('rec_after_fltr')
    CONF_SELECT_QUERY = JSON_CONTENT[CONF_TABLE_NM]['select_fields_query']
    CONF_TGT_CNT_SQL = JSON_CONTENT[CONF_TABLE_NM]['tgt_cnt_query']
    CONF_INS_TO_TEMP_TABLE_SQL = JSON_CONTENT[CONF_TABLE_NM].get('insert_to_tmp_table')  
    if CONF_TGT_TBL_NM == 'invstmt_fund_trx' or CONF_TGT_TBL_NM == 'invstmt_fund_asset':
        CONF_NEGATE_CANCEL_UPD_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('negate_cancel_upd').format(CONF_BATCH_DATE)
        CONF_CANCEL_UPD_REVERSAL_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('cancel_and_update_reversal')
        CONF_SECOND_LOAD_UPD_NOMATCH = JSON_CONTENT[CONF_TABLE_NM].get('second_load_upd_nomatch')
        CONF_LATEST_TGT_REC_NEGATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('latest_tgt_rec_negate')
        CONF_SECOND_LOAD_UPD_MATCH = JSON_CONTENT[CONF_TABLE_NM].get('second_load_upd_match')
        CONF_DELETE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('delete_query').format(CONF_BATCH_ID)
    if CONF_TGT_TBL_NM == 'plcy_trx_sumry':
        CONF_SRC_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('src_query')
    if CONF_FILE_TYPE.lower()=='full' and CONF_TYPE ==2:
        CONF_FULL_FILE_INACTIVE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('full_file_inactive_qry')

    BATCH_DATE = CONF_BATCH_DATE
    # if CONF_TGT_TBL_NK :
    #     drop_dup_on_cols = CONF_TGT_TBL_NK.split(',')
    unpersist_dfs =[]  
    unpersist_lkp_dfs =[]
   
    src_cnt, src_fltr_cnt = read_source_file(CONF_SRC_FILE_PATH,CONF_FILE_DELIMITER)
    print(f"Source file data read successfully")

    if src_cnt == 0 and CONF_TGT_TBL_NM !='plcy_trx_sumry':
        print("No data from source !!!")
    elif src_fltr_cnt==src_cnt and CONF_TGT_TBL_NM !='plcy_trx_sumry':
        print("All src data is filtered out!!!")
        fltr_cnt = src_fltr_cnt
    else :
        # Checking if correct type is passed and truncating temp table in case of type1 table
        CONF_TRUNCATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('truncate_query')
        if CONF_TRUNCATE_QUERY:
            TRUNCATE_QUERY = CONF_TRUNCATE_QUERY.format(CONF_SCHEMA,CONF_JOB_NAME)
            pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, TRUNCATE_QUERY, CONF_CERT_FILE, CONF_BUCKETNAME, CONF_REGION)
            print("Truncate query exeution completed ! ")
            print(TRUNCATE_QUERY)
           
        if CONF_TYPE == 0:
            pass
        elif CONF_TYPE == 1:
            CONF_IDENTIFR_QUERY = JSON_CONTENT[CONF_TABLE_NM]['identifier_query']
            if CONF_TGT_TBL_NM != 'invstmt_fund_trx':
                TMP_CONF_UPDATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('update_query')
                CONF_UPDATE_QUERY = TMP_CONF_UPDATE_QUERY.format(CONF_SCHEMA,CONF_JOB_NAME)
                pass
        elif CONF_TYPE == 2:
            CONF_IDENTIFR_QUERY = JSON_CONTENT[CONF_TABLE_NM]['identifier_query']
            CONF_SCD_2_UPDATE = JSON_CONTENT[CONF_TABLE_NM]['scd_2_update']
            SCD_2_UPDATE_QUERY = CONF_SCD_2_UPDATE.format(CONF_SCHEMA , CONF_JOB_NAME,CONF_BATCH_DATE,CONF_BATCH_ID)
            CONF_ENTITY_TYPE = JSON_CONTENT[CONF_TABLE_NM].get('entity_type')
            CONF_ENTITY_JOB_NAME = CONF_JOB_NAME +'-'+CONF_ENTITY_TYPE

            RESTART_DELETE = JSON_CONTENT[CONF_TABLE_NM].get('restart_delete')
            RESTART_UPDATE = JSON_CONTENT[CONF_TABLE_NM].get('restart_update')
            if RESTART_DELETE and RESTART_UPDATE :
                print("custom delete and update run ")
                CONF_RESTART_DELETE = RESTART_DELETE.format(CONF_BATCH_ID,CONF_JOB_NAME)
                CONF_RESTART_UPDATE = RESTART_UPDATE.format(CONF_BATCH_DATE,CONF_JOB_NAME)
                print(CONF_RESTART_DELETE)
                print("delete start")
                pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, CONF_RESTART_DELETE, CONF_CERT_FILE,CONF_BUCKETNAME, CONF_REGION)
                print(CONF_RESTART_UPDATE)
                print("delete end and update start ")
                pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, CONF_RESTART_UPDATE, CONF_CERT_FILE,CONF_BUCKETNAME, CONF_REGION)
                print("update end")
               

            else :
                restart_job('SCD2', CONF_SCHEMA, CONF_TGT_TBL_NM, CONF_SUR_KEY, \
                CONF_BATCH_DATE, CONF_BATCH_ID, CONF_ENTITY_JOB_NAME, DBUSER, DBHOST, DBPORT, \
                DBNAME, DBPWD, CONF_CERT_FILE, CONF_BUCKETNAME, CONF_REGION,CONF_SRC_SYS)
        else :
            raise Exception("Invalid Type ::"+CONF_TYPE+" !!!")

        # Creating DFs for target table and applying transformation
        src_cnt_first, fltr_cnt_first, rej_cnt_first, tgt_cnt_first = load_ods_table()
        for df in unpersist_dfs:
            df.unpersist()
        if (CONF_TGT_TBL_NM == 'invstmt_fund_trx' and CONF_FILE_TYPE.lower() == 'time_trade') or (CONF_TGT_TBL_NM == 'invstmt_fund_asset' and CONF_FILE_TYPE.lower() == 'time_asset'):
            print("second load started")
            src_cnt_final, fltr_cnt_final, rej_cnt_final, tgt_cnt_final = load_ods_table(load_type='second_load')
            src_cnt = src_cnt_final
            fltr_cnt = fltr_cnt_first + fltr_cnt_final
            rej_cnt = rej_cnt_first + rej_cnt_final
            tgt_cnt = tgt_cnt_final
 
    audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
    CONF_ODS_AUD""" Common job for Saleconnect ODS tables """

import sys
import json
import ast
import re
from datetime import datetime
from awsglue.transforms import ApplyMapping, SelectFields, ResolveChoice, DropNullFields
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark import StorageLevel
from pyspark.sql.functions import col
import boto3
from reusable_functions_sfmgn import err_log_entry
from connect import get_postgres_vw, get_postgres_vw_partn, get_secret, audit_sumry,pg_insert, restart_job, restart_job_pipeline, PrintException

# Capturing start time of the job
v_start_time = datetime.utcnow()
start_time=v_start_time.strftime("%Y-%m-%d %H:%M:%S")
# print(start_time)


args = getResolvedOptions(sys.argv, ['JOB_NAME','region','secret_name','param_file_name',\
'config_bucket_name','ods_cat_db_name','ods_aud_cat_tb_name','config_subdir',\
'job_stat_completion','job_stat_failure','sns_arn','run_env','cert_file_name','stage_bucket_name',\
'delimiter','ods_table_name','file_type','envision_param_file_name','trade_event_src_sys','trade_time_src_sys'])

# Initialize global variables
src_cnt, src_fltr_cnt, rej_cnt, fltr_cnt, tgt_cnt = 0,0,0,0,0

# Define dynamicframe and spark dataframe
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

CONF_JOB_NAME = args['JOB_NAME']
CONF_REGION = args['region']
CONF_TABLE_NM = args['ods_table_name']
CONF_FILE_TYPE = args['file_type']
SSC_CONF_PARAM_FILE_NM = args['param_file_name']
ENV_CONF_PARAM_FILE_NM= args['envision_param_file_name']
CONF_SUBDIR = args['config_subdir']
CONF_STG_BKT_NM = args['stage_bucket_name']
CONF_BUCKETNAME = args['config_bucket_name']
CONF_ODS_CAT_DB_NAME = args['ods_cat_db_name']
CONF_ODS_AUD_CAT_TB_NAME = args['ods_aud_cat_tb_name']
CONF_CERT_FILE = args['cert_file_name']
CONF_FAIL_STAT = args['job_stat_failure']
SNS_ARN= args['sns_arn']
RUN_ENV = args['run_env']
CONF_RUN_STAT = args['job_stat_completion']
CONF_FILE_DELIMITER = args['delimiter']
if CONF_FILE_TYPE == 'event_trade':
    CONF_ERR_LOG_SRC_SYS = args['trade_event_src_sys']
elif CONF_FILE_TYPE == 'time_trade':
    CONF_ERR_LOG_SRC_SYS = args['trade_time_src_sys']
elif CONF_FILE_TYPE == 'maintenance':
    CONF_ERR_LOG_SRC_SYS = args['trade_time_src_sys']
job = Job(glueContext)
job.init(CONF_JOB_NAME, args)

# Read JSON Parameter file
s3 = boto3.resource('s3')

# CONTENT_OBJECT = s3.Object(CONF_BUCKETNAME, str(CONF_SUBDIR) + str(CONF_PARAM_FILE_NM))
# FILE_CONTENT = CONTENT_OBJECT.get()['Body'].read().decode('utf-8')

# Define DB Parameters
CONF_SECRET_NM = args['secret_name']
SECRET = get_secret(CONF_SECRET_NM,CONF_REGION)
DBNAME = SECRET['dbname']
DBPORT = SECRET['port']
DBUSER = SECRET['username']
DBPWD = SECRET['password']
DBHOST = SECRET['host']
DB_URL = "jdbc:postgresql://"+str(DBHOST)+":"+str(DBPORT)+"/"+str(DBNAME)

# if CONF_TYPE == 0:
#     pass
# elif CONF_TYPE == 1:
#     CONF_TRUNCATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('truncate_query')
#     if CONF_TRUNCATE_QUERY:
#         TRUNCATE_QUERY = CONF_TRUNCATE_QUERY.format(CONF_SCHEMA)
#     CONF_IDENTIFR_QUERY = JSON_CONTENT[CONF_TABLE_NM]['identifier_query']
#     CONF_UPDATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('update_query')
# else :
#     CONF_IDENTIFR_QUERY = JSON_CONTENT[CONF_TABLE_NM]['identifier_query']
#    # CONF_TGT_SUR_ID_COL = JSON_CONTENT[CONF_TABLE_NM]['tgt_sur_id_col']
#     CONF_SCD_2_UPDATE = JSON_CONTENT[CONF_TABLE_NM]['scd_2_update']
#     SCD_2_UPDATE_QUERY = CONF_SCD_2_UPDATE.format(CONF_SCHEMA,'2025-05-08' , CONF_JOB_NAME) #CONF_BATCH_DATE

# Function to send SNS notification for missing transaction codes
def send_trx_alert(record_count, missing_codes):
    try:
        print("In send_trx_alert function try block")
        sns_client = boto3.client('sns', region_name='us-east-1')
       
        SUB = f"Action Required - PMF New Transaction Codes in {RUN_ENV}"
       
        MSG = f"{record_count} new transaction code/codes (trx_cd) have been received that are not present in the trx_mastr table.\n\n"
        MSG += f"Transaction Code/Codes: {missing_codes}\n\n"
        MSG += "These transactions have been loaded with Transaction Master ID = '-99'.\n"
        MSG += "Please query the 'abc_err_log' table to review the newly received trx_cd values and update the Transaction Master accordingly.\n"
       
        response = sns_client.publish(
            TopicArn = SNS_ARN,
            Message = f"{MSG}",
            Subject = f"{SUB}"
        )
       
        print(f"SNS Response: {response}")
        return True
       
    except Exception as e:
        print("Exception in sending sns notification: ", e)
        raise e
       
def send_acct_typ_alert(record_count, missing_codes):
    try:
        print("In send_acct_typ_alert function try block")
        sns_client = boto3.client('sns', region_name='us-east-1')

        SUB = f"Action Required - PMF New Account Type Codes in {RUN_ENV}"

        MSG = f"{record_count} new account type code/codes (acct_typ_cd) have been received that are not present in the ACCT_TYP_MSTR table.\n\n"
        MSG += f"Account Type Code/Codes: {missing_codes}\n\n"
        MSG += "These records have NOT been loaded and require review.\n"
        MSG += "Please query the 'abc_err_log' table to review the newly received acct_typ_cd values and update the Account Type Master accordingly.\n"

        response = sns_client.publish(
            TopicArn=SNS_ARN,
            Message=f"{MSG}",
            Subject=f"{SUB}"
        )
        print(f"SNS Response: {response}")
        return True

    except Exception as e:
        print("Exception in sending sns notification: ", e)
        raise e

def send_acct_typ_dq_alert(record_count, reason_summary):
    try:
        print("In send_acct_typ_dq_alert function try block")
        sns_client = boto3.client('sns', region_name='us-east-1')

        SUB = f"Action Required - PMF Account Type Data Quality Failures in {RUN_ENV}"

        MSG = f"{record_count} record(s) failed account type data quality validation and were NOT loaded.\n\n"
        MSG += f"Failure Reason Breakdown:\n{reason_summary}\n\n"
        MSG += "Please query the 'abc_err_log' table to review the flagged records and correct source data accordingly.\n"

        response = sns_client.publish(
            TopicArn=SNS_ARN,
            Message=f"{MSG}",
            Subject=f"{SUB}"
        )
        print(f"SNS Response: {response}")
        return True

    except Exception as e:
        print("Exception in sending sns notification: ", e)
        raise e      
 
# Function to read file from S3
def read_from_s3(bucket_name, file_key):
    """Read file content from an S3 bucket."""
    try:
        s3_client = boto3.client('s3')
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_obj['Body'].read().decode('utf-8')
        print(f"{file_key} read successfully")
        return file_content
    except Exception as e:
        print(f"Error reading param {file_key} from S3: {e}")
        audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
        CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
       src_cnt, tgt_cnt, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
        print("Audit entry successful!")
        sys.exit(e)



# Reading source file
def read_source_file(CONF_SRC_FILE_PATH,CONF_FILE_DELIMITER):
    try:
        global src_cnt,src_fltr_cnt
        if CONF_TGT_TBL_NM != 'plcy_trx_sumry':
            print(f"Reading {CONF_SRC_FILE_PATH} delimete {CONF_FILE_DELIMITER}")
            src_df = glueContext.read.format('csv').option("delimiter", CONF_FILE_DELIMITER).option("quote", "\"").option("escape", "\"").options(header = 'true').load(CONF_SRC_FILE_PATH)
            src_df.persist(StorageLevel.MEMORY_AND_DISK)
            src_df.createTempView("src_file_vw")
            src_df.show(20)
            src_cnt = src_df.count()
            print("Source data as read from file with count:", src_cnt)
        else:
            conf_src_query = CONF_SRC_QUERY.format(CONF_SCHEMA,CONF_BATCH_ID)
            print("conf_src_query: ",conf_src_query)
            src_df = get_postgres_vw(glueContext,conf_src_query, DB_URL, DBUSER, DBPWD)
            src_df.persist(StorageLevel.MEMORY_AND_DISK)
            src_df.show(20)
            src_cnt = src_df.count()
            print("Source data as read from file with count:", src_cnt)
        # Applying filter condition on source df if applicable
        if CONF_SRC_FLTR_QUERY and CONF_TGT_TBL_NM != 'plcy_trx_sumry':
            if CONF_TGT_TBL_NM == 'invstmt_fund_trx' or CONF_TGT_TBL_NM =='invstmt_fund_asset':
                conf_src_fltr_query = CONF_SRC_FLTR_QUERY.format(CONF_BATCH_DATE)
            else:
                conf_src_fltr_query = CONF_SRC_FLTR_QUERY.format(CONF_SCHEMA)
            print("conf_src_fltr_query: ",conf_src_fltr_query)
            src_df = spark.sql(conf_src_fltr_query)
            src_df.persist(StorageLevel.MEMORY_AND_DISK)
            src_df.show(20)
            src_fltr_df_cnt = src_df.count()
            print("After running src_filter query,source data count:", src_fltr_df_cnt)
            if CONF_MLTI_TGT_FLAG:
                src_cnt = src_fltr_df_cnt
            src_fltr_cnt = src_cnt - src_fltr_df_cnt
            print("Total records filtered from source:", src_fltr_cnt)
            src_df.createTempView("src_vw")
        if CONF_TGT_TBL_NM == 'plcy_trx_sumry':
            conf_src_fltr_query = CONF_SRC_FLTR_QUERY.format(CONF_SCHEMA,CONF_BATCH_ID)
            print("conf_src_fltr_query: ",conf_src_fltr_query)
            src_df = get_postgres_vw(glueContext,conf_src_fltr_query, DB_URL, DBUSER, DBPWD)
            src_df.persist(StorageLevel.MEMORY_AND_DISK)
            src_df.show(20)
            src_fltr_df_cnt = src_df.count()
            print("After running src_filter query,source data count:", src_fltr_df_cnt)
            src_df.createTempView("src_vw")

        return src_cnt, src_fltr_cnt
    except Exception as e:
        print(f"Error reading source file from S3: {e}")
        audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
        CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
        src_cnt, tgt_cnt, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
        print("Audit entry successful!")
        sys.exit(e)


def src_tgt_mapping(ods_cat_db_name,ods_aud_cat_tb_name,job_name,tgt_file_nm,src_file_nm,CONF_BATCH_ID,src_sys,src_df,catalog_db,catalog_table):
  try:
    string_schema=str(src_df.schema())
    string_schema = string_schema.split('[')[1]    

    clean_list = list(map(lambda x: x.replace("Field","").replace("StructType","") \
                .replace("({})","").replace("{}","").replace("({}","") \
                .replace("("," ").replace(", "," "),string_schema.split('),')))
    input_list=[x.strip() for x in clean_list  if "Type" in x ]
    output_list=[]
    for i in range(len(input_list)):
        separator=str(input_list[i]).split(' ')
        separator[1]=separator[1].split('Type')
        if(separator[1][0]=="Decimal"):
            output_list.append(str(separator[0])+'*'+ str(separator[1][0]+'('+ separator[2]+','+separator[3]+')').lower())
        else:
            output_list.append(str(separator[0])+'*'+ str(separator[1][0]).lower())

    src_list=list(map(lambda x: x.capitalize(),output_list))        
    src_list.sort()
    print(f"Source List: {src_list}and len:{len(src_list)}")

    glue=boto3.client('glue')
    catalog_list=[]
    response = glue.get_table(DatabaseName=catalog_db,Name=catalog_table)
    src_col_nm = [col.split('*')[0].upper() for col in src_list]

    for i in response['Table']['StorageDescriptor']['Columns']:
      if i['Name'].upper() in src_col_nm:
        catalog_list.append(i['Name']+'*'+i['Type'])
    catalog_list=list(map(lambda x: x.capitalize(),catalog_list))
    catalog_list.sort()
    print(f"Catalog List {catalog_list}and len:{len(catalog_list)}")

    for i in range(len(src_list)):
        src_list[i]=str(src_list[i])+'*'+str(catalog_list[i])
    temp=[]
    for i in src_list:
        temp.append(i.split('*'))
    final_map=list(map(tuple,temp))
    return final_map
  except Exception as err:
    print(f"Failed to create apply map: {err}")
    audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
    CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
    src_cnt, tgt_cnt, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
    print("Audit entry successful!")
    sys.exit(err)


def stg_file_to_table_load(job_name,tgt_file_nm,src_file_nm,CONF_BATCH_ID,tgt_load_df,src_sys,ods_cat_db_name,ods_aud_cat_tb_name,ods_cat_tgt_tb_nm):
    try:
        tgt_count = tgt_load_df.count()
        if tgt_count !=0:
            print("Records for new insert: ",tgt_count)
            #Convert df to Glue dynamic frame
            src_dyn_frm = DynamicFrame.fromDF(tgt_load_df, glueContext, "src_dyn_frm")

            map_fields =src_tgt_mapping(ods_cat_db_name,ods_aud_cat_tb_name,job_name,tgt_file_nm,src_file_nm,CONF_BATCH_ID,src_sys,src_dyn_frm,ods_cat_db_name,ods_cat_tgt_tb_nm)
            map_fields = [tuple(item.upper() if ind in (0,2) else item for ind,item in enumerate(my_tuple) ) for my_tuple in map_fields]
            print(f"map_fields:{map_fields}")

            ## Apply map operation so Glue will understand its input and output mapping
            final_applymapping = ApplyMapping.apply(frame = src_dyn_frm, mappings = map_fields, transformation_ctx = "final_applymapping")
            print('Apply mapping is completed!')

            final_resolvechoice = ResolveChoice.apply(frame = final_applymapping, \
            choice = "MATCH_CATALOG", database = ods_cat_db_name, table_name = ods_cat_tgt_tb_nm, \
            transformation_ctx = "final_resolvechoice")

            final_resolvechoice1 = ResolveChoice.apply(frame = final_resolvechoice, \
            choice = "make_cols", transformation_ctx = "final_resolvechoice1")

            dyf_dropNullfields = DropNullFields.apply(frame = final_resolvechoice1)

            datasink = glueContext.write_dynamic_frame.from_catalog(frame = dyf_dropNullfields, \
            database = ods_cat_db_name, table_name = ods_cat_tgt_tb_nm,  \
            transformation_ctx = "datasink")
            print("Target table loaded successfully!")
    except Exception as e:
            print(f"Unable to load table {ods_cat_tgt_tb_nm}: {e}")
            audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
    CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
    src_cnt, 0, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
            print("Audit entry successful!")
            sys.exit(e)



def load_ods_table(load_type=None):
    try:
        global src_cnt,src_fltr_cnt,fltr_cnt,rej_cnt,tgt_cnt
        print(f'load_type:{load_type}')
        if (CONF_TGT_TBL_NM =='invstmt_fund_trx' or CONF_TGT_TBL_NM =='invstmt_fund_asset') and load_type!='second_load':
           print(CONF_DELETE_QUERY)
           pg_insert(DBUSER,DBHOST,DBPORT,DBNAME,DBPWD,CONF_DELETE_QUERY,CONF_CERT_FILE,CONF_BUCKETNAME, CONF_REGION)
           print(f"current batch records are deleted for {CONF_TGT_TBL_NM.upper()}")
        # Reading current target table data
        CURR_TBL_QUERY = CONF_CURR_TBL_QUERY.format(CONF_SCHEMA)
        curr_df = get_postgres_vw_partn(glueContext,CURR_TBL_QUERY,DB_URL,DBUSER,DBPWD,CONF_SUR_KEY,None,None,None)
        print(f" Current target table {CONF_TGT_TBL_NM} read successfully ")
        curr_df.createOrReplaceTempView('curr_vw')
        curr_tbl_count = curr_df.count()
        print("curr_tbl_count: ", curr_tbl_count)
        curr_df.show(5)

        # Reading Required Tables
        if CONF_LKP_TABLES and load_type !='second_load':
            print("----- Reading required tables for lookup! -----")
            for i in range(int(CONF_LKP_TABLES)) :
                TABLE = 'table_'+str(i+1)
                if CONF_TGT_TBL_NM == 'plcy_trx_sumry':
                    CONF_TBL_QUERY = (JSON_CONTENT[CONF_TABLE_NM][TABLE]).format(CONF_SCHEMA,CONF_BATCH_DATE,CONF_BATCH_ID)
                else:
                    CONF_TBL_QUERY = (JSON_CONTENT[CONF_TABLE_NM][TABLE]).format(CONF_SCHEMA)
                table_df = get_postgres_vw(glueContext,CONF_TBL_QUERY,DB_URL,DBUSER,DBPWD)
                table_df.createOrReplaceTempView(TABLE+'_vw')
                #table_df.persist(StorageLevel.MEMORY_AND_DISK)
                table_df.show(5)
                lkp_tbl_cnt = table_df.count()
                #unpersist_dfs.append(table_df)
                print(f"View Created for {TABLE}, total count from ref table: {lkp_tbl_cnt}")


        # Generating intermediate table views
        if CONF_TEMP_TABLES and load_type !='second_load':
            print("----- Generating intermediate temp tables for lookup! -----")

            for i in range(int(CONF_TEMP_TABLES)) :
                TABLE = 'temp_table_'+str(i+1)
                CONF_TBL_QUERY = (JSON_CONTENT[CONF_TABLE_NM][TABLE])
                temp_table_df = spark.sql(CONF_TBL_QUERY)
                temp_table_df.createOrReplaceTempView(TABLE+'_vw')
                #temp_table_df.persist(StorageLevel.MEMORY_AND_DISK)
                temp_table_df.show(10)
                temp_tbl_cnt = temp_table_df.count()
                #unpersist_dfs.append(temp_table_df)
                if CONF_TGT_TBL_NM == 'invstmt_fund_trx' and i+1 == 2:
                    print(f"CONF_SRC_FILE_NAME: {CONF_SRC_FILE_NAME}")
                    if CONF_ERR_LOG_SRC_SYS == 'SSC_EVENT_ENV_TRADE':
                        print(f"Processing Error Logs for system: {CONF_ERR_LOG_SRC_SYS}")
                        CONF_ABC_ERR_LOG_QUERY_TRADE_EC25 = CONF_SRC_FILE_KEY= JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_25', CONF_ABC_ERR_LOG_QUERY_TRADE_EC25,CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD,tgt_tbl_nm=CONF_TGT_TBL_NM)

                        CONF_ABC_ERR_LOG_QUERY_OMNI_EC25 = CONF_SRC_FILE_KEY= JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_25', CONF_ABC_ERR_LOG_QUERY_OMNI_EC25,CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD,tgt_tbl_nm=CONF_TGT_TBL_NM)
 
 
                        CONF_ABC_ERR_LOG_QUERY_TRADE = CONF_SRC_FILE_KEY= JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_trade']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_26', CONF_ABC_ERR_LOG_QUERY_TRADE,CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD,tgt_tbl_nm=CONF_TGT_TBL_NM)
 
                        CONF_ABC_ERR_LOG_QUERY_OMNI = CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_omni']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_26', CONF_ABC_ERR_LOG_QUERY_OMNI,CONF_ERR_LOG_SRC_SYS,CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD,tgt_tbl_nm=CONF_TGT_TBL_NM)
                    else:
                        print(f"Processing Error Logs for system: {CONF_ERR_LOG_SRC_SYS}")
                        CONF_ABC_ERR_LOG_QUERY = CONF_SRC_FILE_KEY= JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query']
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_25', CONF_ABC_ERR_LOG_QUERY,CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME,CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD)
 
                        CONF_ABC_ERR_LOG_QUERY_TIMEBASED = CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_timebased']
           
                        err_log_entry(spark, glueContext, temp_table_df, 'EC_26',CONF_ABC_ERR_LOG_QUERY_TIMEBASED, CONF_ERR_LOG_SRC_SYS, CONF_SRC_FILE_NAME, CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME, DB_URL, DBUSER, DBPWD)
           
                    print("Initiating check for missing trx_cd against master table for SNS Alert...")
                    try:
                        CONF_MISSING_TRX_ALERT_QUERY = JSON_CONTENT[CONF_TABLE_NM]['missing_trx_alert_query']
               
                        missing_trx_df = spark.sql(CONF_MISSING_TRX_ALERT_QUERY)
                        missing_count = missing_trx_df.count()
               
                        if missing_count > 0:
                            missing_rows = missing_trx_df.select("TRX_CD").distinct().collect()
                            codes_list = [str(row['TRX_CD']) for row in missing_rows]
                            codes_string = ", ".join(codes_list)
                   
                            print(f"Found {missing_count} missing transaction codes. Triggering SNS alert...")
                   
                            send_trx_alert(missing_count, codes_string)
                        else:
                            print("No missing transaction codes found. No alert needed.")
                   
                    except Exception as e:
                        print(f"Exception occurred while checking missing trx_cd or sending alert: {e}")
       
                print(f"View Created for {TABLE}, total count from temp table: {temp_tbl_cnt}")

        print(f"DEBUG: CONF_TGT_TBL_NM={CONF_TGT_TBL_NM}, CONF_TABLE_NM={CONF_TABLE_NM}, CONF_TEMP_TABLES={CONF_TEMP_TABLES}")
        print(f"CONF_FILE_TYPE: {CONF_FILE_TYPE}")      
        if CONF_TGT_TBL_NM == 'pty_detl':
                print(f"CONF_SRC_FILE_NAME: {CONF_SRC_FILE_NAME}")

                # ---------- New ACCT_TYP_CD ----------
                print("Initiating check for missing acct_typ_cd against master table for SNS Alert...")
                try:
                    CONF_MISSING_ACCT_TYP_ALERT_QUERY = JSON_CONTENT[CONF_TABLE_NM]['missing_acct_typ_alert_query']
                    missing_acct_typ_df = spark.sql(CONF_MISSING_ACCT_TYP_ALERT_QUERY)
                    missing_acct_typ_count = missing_acct_typ_df.count()

                    if missing_acct_typ_count > 0:
                        missing_rows = missing_acct_typ_df.select("ACCT_TYP_CD").distinct().collect()
                        codes_list = [str(row['ACCT_TYP_CD']) for row in missing_rows]
                        codes_string = ", ".join(codes_list)

                        print(f"Found {missing_acct_typ_count} unexpected account type codes. Triggering SNS alert...")
                        send_acct_typ_alert(missing_acct_typ_count, codes_string)

                        # Log to error table (rule 6's own err_log query)
                        CONF_ABC_ERR_LOG_QUERY_ACCT_TYP = JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_acct_typ']
                        err_log_entry(spark, glueContext, missing_acct_typ_df, 'EC_27',
                                      CONF_ABC_ERR_LOG_QUERY_ACCT_TYP, CONF_ERR_LOG_SRC_SYS,
                                      CONF_SRC_FILE_NAME, CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME,
                                      DB_URL, DBUSER, DBPWD, tgt_tbl_nm=CONF_TGT_TBL_NM)
                   
                    else:
                          print("No unexpected account type codes found. No alert needed.")
                except Exception as e:
                       print(f"Exception occurred while checking missing acct_typ_cd or sending alert: {e}")

                print("Initiating account type data quality validation...")
                acct_typ_bad_keys_df = None
                try:
                    CONF_ACCT_TYP_DQ_QUERY = JSON_CONTENT[CONF_TABLE_NM]['acct_typ_dq_query']
                    acct_typ_dq_df = spark.sql(CONF_ACCT_TYP_DQ_QUERY)
                    acct_typ_dq_df.persist(StorageLevel.MEMORY_AND_DISK)
                    acct_typ_dq_count = acct_typ_dq_df.count()

                    if acct_typ_dq_count > 0:
                        reason_rows = acct_typ_dq_df.groupBy("DQ_REASON_CD").count().collect()
                        reason_summary = "\n".join(
                                [f"{row['DQ_REASON_CD']}: {row['count']} record(s)" for row in reason_rows]
                            )
                        print(f"Found {acct_typ_dq_count} records failing account type validation. Triggering SNS alert...")
                        send_acct_typ_dq_alert(acct_typ_dq_count, reason_summary)

                        acct_typ_dq_df.createOrReplaceTempView("temp_dq_vw")
                        CONF_ABC_ERR_LOG_QUERY_ACCT_TYP_DQ = JSON_CONTENT[CONF_TABLE_NM]['abc_err_log_query_acct_typ_dq']
                        err_log_entry(spark, glueContext, acct_typ_dq_df, 'EC_28',
                                    CONF_ABC_ERR_LOG_QUERY_ACCT_TYP_DQ, CONF_ERR_LOG_SRC_SYS,
                                    CONF_SRC_FILE_NAME, CONF_BATCH_ID, CONF_BATCH_DATE, CONF_JOB_NAME,
                                    DB_URL, DBUSER, DBPWD, tgt_tbl_nm=CONF_TGT_TBL_NM)

                        acct_typ_bad_keys_df = acct_typ_dq_df.select("pty_uniq_id").distinct()

                    else:
                        print("No account type data quality issues found. No records excluded.")

                    acct_typ_dq_df.unpersist()
                except Exception as e:
                        print(f"Exception occurred while validating acct_typ_cd or sending alert: {e}")      


        # droping duplicates if required based on provided columns
        if load_type !='second_load':
            # Executing lookup query to fetch the lookup column values
            print("----- Executing Lookup Query! -----")
            if CONF_TGT_TBL_NM =='invstmt_fund_trx' or CONF_TGT_TBL_NM =='invstmt_fund_asset' :
                CONF_LKP_QUERY = JSON_CONTENT[CONF_TABLE_NM]['lkp_query'].format(CONF_BATCH_DATE)
            else:
                CONF_LKP_QUERY = JSON_CONTENT[CONF_TABLE_NM]['lkp_query']
            stage_transformed_df1 = spark.sql(CONF_LKP_QUERY)
            #stage_transformed_df1.createTempView("lkp_vw")
            #stage_transformed_df1.persist(StorageLevel.MEMORY_AND_DISK)
            stage_transformed_df1.show(20)
            lkp_cnt = stage_transformed_df1.count()
            print(f"After lookup, count {lkp_cnt}")
            if CONF_TGT_TBL_NK :
                drop_dup_on_cols = CONF_TGT_TBL_NK.split(',')
                stage_transformed_df1 = stage_transformed_df1.dropDuplicates(drop_dup_on_cols)
            stage_transformed_df1.persist(StorageLevel.MEMORY_AND_DISK)
            stdf1_cnt = stage_transformed_df1.count()
            unpersist_lkp_dfs.append(stage_transformed_df1)
            stage_transformed_df1.show()
            stage_transformed_df1.createOrReplaceTempView("lkp_vw")

            dup_cnt = src_cnt - src_fltr_cnt - stdf1_cnt
            print('Total Records Dropped after duplicate check:', dup_cnt)
   
                # proceding only if records are present after rejecting
            if stdf1_cnt == 0 :
                tgt_cnt = 0
                print("Data count zero. Nothing to load !!!")
            else :
                if CONF_TYPE in (1,2):
                    #  Calculating Hashkey on src side. Also on tgt side in case of type 1
                    identifier_df = spark.sql(CONF_IDENTIFR_QUERY)
                   
                else :
                    identifier_df = stage_transformed_df1

                identifier_df.persist(StorageLevel.MEMORY_AND_DISK)
                #identifier_cnt = identifier_df.count()
                unpersist_lkp_dfs.append(identifier_df)
                #print(f'identifier_df count:{identifier_cnt}')
                # print("identifier_df:")
                identifier_df.show(10)

                # filtering out data
                identifier_df.createTempView("idntifr_df_vw")  
                REC_FLTR_QUERY = CONF_REC_AFTER_FLTR.format(CONF_BATCH_ID,CONF_JOB_NAME,CONF_BATCH_DATE)
                rec_after_fltr_df = spark.sql(REC_FLTR_QUERY)
                #rec_after_fltr_df.persist(StorageLevel.MEMORY_AND_DISK)
                #unpersist_lkp_dfs.append(rec_after_fltr_df)
                rec_after_fltr_df.createTempView("rec_after_fltr_df_vw")
                #rec_after_fltr_cnt = rec_after_fltr_df.count()
                #print(f"rec_after_fltr_df count:{rec_after_fltr_cnt}")
                print(f"rec_after_fltr_df")
                rec_after_fltr_df.show(10)
                if CONF_FILE_TYPE.lower() == 'full' and CONF_TYPE==2:
                    CONF_FULL_FILE_INACTIVE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('full_file_inactive_qry')
                    CONF_FULL_FILE_INACTIVE_QUERY = CONF_FULL_FILE_INACTIVE_QUERY.format(CONF_BATCH_ID,CONF_JOB_NAME,CONF_BATCH_DATE)
                    full_file_inactive_df = spark.sql(CONF_FULL_FILE_INACTIVE_QUERY)
                    print(f'CONF_FULL_FILE_INACTIVE_QUERY:{CONF_FULL_FILE_INACTIVE_QUERY}')
                    full_file_inactive_df.show()
                    rec_after_fltr_df = rec_after_fltr_df.union(full_file_inactive_df)

                #rec after flter cnt , idntfir cnt prints are removed

                # separating insert and update records in case of type 1 and creating final dataframe
                if CONF_TYPE == 1 and (CONF_TGT_TBL_NM != 'invstmt_fund_trx' and CONF_TGT_TBL_NM != 'invstmt_fund_asset'):
                     temp_sink_df = rec_after_fltr_df.where(col('INSERT_UPDATE_FLG') == 'U')
                     temp_sink_df.persist(StorageLevel.MEMORY_AND_DISK)
     
                     # Checking if there are any updates and calling map and table load if updates exist
                     if temp_sink_df.rdd.isEmpty() :
                         upd_tgt_cnt = 0
                         print("Nothing to Update...")
                     else :
                         upd_tgt_cnt = temp_sink_df.count()
                         print(f"{upd_tgt_cnt} Updates detected. Performing mapping and table load operation")
                         temp_sink_df.createOrReplaceTempView("tgt_sink_df_vw")
                         SELECT_QUERY = CONF_SELECT_QUERY.format(CONF_BATCH_ID,CONF_JOB_NAME,CONF_BATCH_DATE)
                         tmp_tgt_load_df = spark.sql(SELECT_QUERY)
                         stg_file_to_table_load(CONF_JOB_NAME,CONF_SRC_FILE_NAME,CONF_SRC_FILE_NAME,CONF_BATCH_ID,tmp_tgt_load_df,CONF_SRC_SYS,CONF_ODS_CAT_DB_NAME,CONF_ODS_AUD_CAT_TB_NAME,CONF_TEMP_UPD_TABLE)
                         pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, CONF_UPDATE_QUERY, CONF_CERT_FILE,CONF_BUCKETNAME, CONF_REGION)
                     tgt_sink_df = rec_after_fltr_df.where(col('INSERT_UPDATE_FLG') == 'I')
                     tgt_sink_df.show(5)
                     ins_tgt_cnt = tgt_sink_df.count()
                     tgt_cnt=ins_tgt_cnt+upd_tgt_cnt
           
                else:
                    tgt_sink_df = rec_after_fltr_df
                    if CONF_TGT_TBL_NM == 'invstmt_fund_asset' and load_type != 'second_load':
                        print("HOLD DEBUG - action codes:", [r[0] for r in tgt_sink_df.select('SC_ASST_POS_ACTN_CD').collect()])
                        tgt_sink_df = tgt_sink_df.where(col('SC_ASST_POS_ACTN_CD') != 'U')
                        print("HOLD DEBUG - rows kept after hold:", tgt_sink_df.count())
                    tgt_sink_df.persist(StorageLevel.MEMORY_AND_DISK)
                    tgt_sink_df.show(5)
                    tgt_cnt = tgt_sink_df.count()
 
        if CONF_TGT_TBL_NM == 'invstmt_fund_trx':
            if load_type == 'second_load':
                stdf1_cnt=0
                print("second time func")
                latest_tgt_rec_negate_df = spark.sql(CONF_LATEST_TGT_REC_NEGATE_QUERY)
                latest_tgt_rec_negate_df.persist(StorageLevel.MEMORY_AND_DISK)
                latest_tgt_rec_negate_cnt = latest_tgt_rec_negate_df.count()
                print(f'Reversal Qualified record count from target table:{latest_tgt_rec_negate_cnt}')
                latest_tgt_rec_negate_df.createOrReplaceTempView("LATEST_TGT_REC_NEGATE_VW")
                for df in unpersist_lkp_dfs:
                    df.unpersist()
                negate_cancel_upd_df = spark.sql(CONF_NEGATE_CANCEL_UPD_QUERY)
                negate_cancel_upd_cnt = negate_cancel_upd_df.count()
                print(f'Records for reversal:{negate_cancel_upd_cnt}')
                latest_tgt_rec_negate_df.unpersist()
                negate_cancel_upd_df.persist(StorageLevel.MEMORY_AND_DISK)
                negate_cancel_upd_df.createOrReplaceTempView("negate_cancel_upd_vw")
                tgt_sink_df = spark.sql(CONF_CANCEL_UPD_REVERSAL_QUERY)
                if CONF_TGT_TBL_NK :
                    drop_dup_on_cols = CONF_TGT_TBL_NK.split(',')
                    print("Duplicate check columns:",drop_dup_on_cols)
                    tgt_sink_df = tgt_sink_df.dropDuplicates(drop_dup_on_cols)
                tgt_sink_df.show()
                negate_cancel_upd_df.unpersist()
                tgt_sink_df.persist(StorageLevel.MEMORY_AND_DISK)
                tgt_cnt = tgt_sink_df.count()
                dup_cnt = negate_cancel_upd_cnt - tgt_cnt
                print('Total Records Dropped after duplicate check:', dup_cnt)
                # Checking if there are any inserts and calling map and table load if inserts exist
   

        # NEW — asset second_load in-place update
        if CONF_TGT_TBL_NM == 'invstmt_fund_asset' and load_type == 'second_load':
            stdf1_cnt = 0
            latest_tgt_rec_negate_cnt = 0
            arc_cnt = 0
            print("asset second load - in-place update against persisted rebook")
            upd_match_df = spark.sql(CONF_SECOND_LOAD_UPD_MATCH)
            upd_match_df.persist(StorageLevel.MEMORY_AND_DISK)
            upd_tgt_cnt = upd_match_df.count()
            print(f'asset U records matched to current rebook: {upd_tgt_cnt}')
            upd_match_df.show(5)
            if upd_tgt_cnt == 0:
                print("Nothing to Update in asset second load...")
            else:
                upd_match_df.createOrReplaceTempView("tgt_sink_df_vw")
                SELECT_QUERY = CONF_SELECT_QUERY.format(CONF_BATCH_ID, CONF_JOB_NAME, CONF_BATCH_DATE)
                tmp_tgt_load_df = spark.sql(SELECT_QUERY)
                tmp_tgt_load_df.show(5)
                stg_file_to_table_load(CONF_JOB_NAME, CONF_SRC_FILE_NAME, CONF_SRC_FILE_PATH, CONF_BATCH_ID, tmp_tgt_load_df, CONF_SRC_SYS, CONF_ODS_CAT_DB_NAME, CONF_ODS_AUD_CAT_TB_NAME, CONF_TEMP_UPD_TABLE)
                pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, CONF_UPDATE_QUERY, CONF_CERT_FILE, CONF_BUCKETNAME, CONF_REGION)
            upd_match_df.unpersist()
            tgt_cnt = 0
            # ---- NEW: unmatched held U -> insert as 'O' (spec 1a) ----
            upd_nomatch_df = spark.sql(CONF_SECOND_LOAD_UPD_NOMATCH)
            upd_nomatch_df.persist(StorageLevel.MEMORY_AND_DISK)
            nomatch_cnt = upd_nomatch_df.count()
            print(f"asset lone-U (no match) to insert as O: {nomatch_cnt}")
            if nomatch_cnt > 0:
                upd_nomatch_df.createOrReplaceTempView("tgt_sink_df_vw")
                INS_SELECT_QUERY = CONF_SELECT_QUERY.format(CONF_BATCH_ID, CONF_JOB_NAME)
                ins_load_df = spark.sql(INS_SELECT_QUERY)
                if CONF_TGT_TBL_NM == 'invstmt_fund_asset':
                    ins_load_df = ins_load_df.drop('temp_invstmt_fund_asset_id')
                stg_file_to_table_load(CONF_JOB_NAME, CONF_SRC_FILE_NAME, CONF_SRC_FILE_PATH,
                                    CONF_BATCH_ID, ins_load_df, CONF_SRC_SYS,
                                    CONF_ODS_CAT_DB_NAME, CONF_ODS_AUD_CAT_TB_NAME, CONF_CATALOG_TBL)
            upd_nomatch_df.unpersist()
           

        if tgt_cnt == 0 :
            print("Nothing to Insert...")
        else :
            print(f"{tgt_cnt} Inserts detected. Performing mapping and table load operation...")
            # Selecting required fields in tgt_sink_df
            tgt_sink_df.createOrReplaceTempView("tgt_sink_df_vw")                
            SELECT_QUERY = CONF_SELECT_QUERY.format(CONF_BATCH_ID, CONF_JOB_NAME)
            tgt_load_df = spark.sql(SELECT_QUERY)
            if CONF_TGT_TBL_NM == 'invstmt_fund_asset':                  # NEW
                tgt_load_df = tgt_load_df.drop('temp_invstmt_fund_asset_id')
            tgt_load_df.show(5)
            tgt_sink_df.unpersist()
            stg_file_to_table_load(CONF_JOB_NAME,CONF_SRC_FILE_NAME,CONF_SRC_FILE_NAME,CONF_BATCH_ID,tgt_load_df,CONF_SRC_SYS,CONF_ODS_CAT_DB_NAME,CONF_ODS_AUD_CAT_TB_NAME,CONF_CATALOG_TBL)
        if stdf1_cnt != 0:
            # Running SCD2 update in case of type 2 table
            if CONF_TYPE == 2 :
                #added if newly
                if CONF_TEMP_UPD_TABLE :
                    INS_TO_TEMP_TABLE_SQL = CONF_INS_TO_TEMP_TABLE_SQL.format(CONF_BATCH_ID,CONF_JOB_NAME)
                    tmp_load_df = spark.sql(INS_TO_TEMP_TABLE_SQL)
                    tmp_load_df.persist(StorageLevel.MEMORY_AND_DISK)
                    tmp_df_cnt = tmp_load_df.count()
                    unpersist_lkp_dfs.append(tmp_load_df)
                    if tmp_df_cnt != 0 :
                       stg_file_to_table_load(CONF_JOB_NAME,CONF_SRC_FILE_NAME,CONF_SRC_FILE_NAME,CONF_BATCH_ID,tmp_load_df,CONF_SRC_SYS,CONF_ODS_CAT_DB_NAME,CONF_ODS_AUD_CAT_TB_NAME,CONF_TEMP_UPD_TABLE)
                       print("Data Load to Temp Table completed ! ")
                    else:
                        print("Nothing to Insert to Temp Table !")
                pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, SCD_2_UPDATE_QUERY, CONF_CERT_FILE, CONF_BUCKETNAME, CONF_REGION)        
                if CONF_TGT_GTR_CNT_FLAG :
                    src_cnt = lkp_cnt
        # Getting Filtered and Rejected Counts
        if load_type =='second_load':
            fltr_cnt = latest_tgt_rec_negate_cnt - tgt_cnt
        else:
            fltr_cnt = src_cnt - tgt_cnt
        rej_cnt = src_cnt - fltr_cnt - tgt_cnt

        # Getting Target count
        CONF_TGT_CNT_QUERY = CONF_TGT_CNT_SQL.format(CONF_SCHEMA, CONF_BATCH_ID, CONF_JOB_NAME)
        final_count = get_postgres_vw(glueContext,CONF_TGT_CNT_QUERY, DB_URL, DBUSER, DBPWD)
        tgt_tbl_cnt = final_count.collect()[0][0]

        print("Source Count :", src_cnt)
        print("Filtered Count :", fltr_cnt)
        print("Rejected Count :", rej_cnt)
        print("Target Table Count :", tgt_tbl_cnt)
   
        return src_cnt, fltr_cnt, rej_cnt, tgt_tbl_cnt    
    except Exception as e:
        PrintException()
        #audit sumry entry
        audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
        CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
        src_cnt, 0, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
        print("Audit entry successful!")
        sys.exit(e)

try:
    print(f"Job started running at {start_time}")
    if 'asset' in CONF_FILE_TYPE or 'trade' in CONF_FILE_TYPE:
        CONF_PARAM_FILE_NM = ENV_CONF_PARAM_FILE_NM
    else:
        CONF_PARAM_FILE_NM = SSC_CONF_PARAM_FILE_NM
    FILE_CONTENT = read_from_s3(CONF_BUCKETNAME,str(CONF_SUBDIR) + str(CONF_PARAM_FILE_NM))
    JSON_CONTENT = json.loads(FILE_CONTENT)
    if CONF_FILE_TYPE.lower() == "maintenance":
        CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['maint_src_file_key']
        CONF_SRC_FILE_NAME = JSON_CONTENT[CONF_TABLE_NM]['maint_src_file_name']
        CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_timebased.json"
    elif CONF_FILE_TYPE.lower() == "full":
        CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['full_src_file_key']
        CONF_SRC_FILE_NAME = JSON_CONTENT[CONF_TABLE_NM]['full_src_file_name']
        CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_full_file.json"
    elif CONF_FILE_TYPE.lower() in ('event_trade','event_asset'):
        CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['event_src_file_key']
        CONF_SRC_FILE_NAME = JSON_CONTENT[CONF_TABLE_NM]['event_src_file_name']
        if CONF_FILE_TYPE.lower() == 'event_trade':
            CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_event_trade.json"
        else:
            CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_event_asset.json"
    elif CONF_FILE_TYPE.lower() in ('time_trade','time_asset'):
        CONF_SRC_FILE_KEY = JSON_CONTENT[CONF_TABLE_NM]['time_src_file_key']
        CONF_SRC_FILE_NAME = JSON_CONTENT[CONF_TABLE_NM]['time_src_file_name']
        CONF_PARAM_DYNAMIC_FILE = CONF_SUBDIR + "param_lnd_to_stg_sc_dynamic_timebased.json"
    else:
        print("Please enter valid file type!")
        sys.exit("Please enter valid file type!")

    # Reading dynamic values from param gen
    config_param_lines = read_from_s3(CONF_BUCKETNAME,CONF_PARAM_DYNAMIC_FILE)
    if not config_param_lines:
        sys.exit(f"Error reading config file :{CONF_PARAM_DYNAMIC_FILE} from S3")
    config_param = json.loads(config_param_lines)
    CONF_BATCH_DATE = config_param['Dynamic']['batch_date']
    CONF_BATCH_ID = config_param['Dynamic']['btch_id']
   
    # Retriveing Param Information
    if  CONF_FILE_TYPE.lower() in ('maintenance') and 'customer' in CONF_TABLE_NM:
        CONF_SRC_SYS = JSON_CONTENT['Global']['Static']['customer_source_system']
    else:    
        CONF_SRC_SYS = JSON_CONTENT['Global']['Static']['source_system']
    CONF_SCHEMA = JSON_CONTENT['Global']['Static']['ods_schema_name']
    CONF_CATALOG_TBL = JSON_CONTENT[CONF_TABLE_NM]['catalog_tbl']
    CONF_SRC_FILE_PATH = "s3://" + str(CONF_STG_BKT_NM) + "/" + str(CONF_SRC_FILE_KEY) + str(CONF_SRC_FILE_NAME)
    print(f"Source file {CONF_SRC_FILE_NAME} in {CONF_SRC_FILE_KEY} path in {CONF_STG_BKT_NM}")
    CONF_TYPE = int(JSON_CONTENT[CONF_TABLE_NM]['table_load_type'])
    CONF_TEMP_UPD_TABLE = JSON_CONTENT[CONF_TABLE_NM].get('upd_temp_table_name')
    CONF_TGT_TBL_NM = JSON_CONTENT[CONF_TABLE_NM]['tgt_tbl_name']
    CONF_LKP_TABLES = JSON_CONTENT[CONF_TABLE_NM].get('lkp_tables')
    CONF_TEMP_TABLES = JSON_CONTENT[CONF_TABLE_NM].get('temp_tables')

    CONF_SRC_FLTR_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('src_fltr_query')
    CONF_SRC_FINAL_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('src_final_query')
    CONF_LKP_QUERY = JSON_CONTENT[CONF_TABLE_NM]['lkp_query']
    CONF_TGT_TBL_NK = JSON_CONTENT[CONF_TABLE_NM].get('tgt_tbl_nk')
    CONF_MLTI_TGT_FLAG = JSON_CONTENT[CONF_TABLE_NM].get('multi_tgt_flag')
    CONF_TGT_GTR_CNT_FLAG = JSON_CONTENT[CONF_TABLE_NM].get('tgt_gtr_cnt')
    CONF_CURR_TBL_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('cur_table')
    CONF_SUR_KEY = JSON_CONTENT[CONF_TABLE_NM].get('sur_key')
    CONF_TEMP_TABLE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('temp_table_query')
    CONF_REC_AFTER_FLTR = JSON_CONTENT[CONF_TABLE_NM].get('rec_after_fltr')
    CONF_SELECT_QUERY = JSON_CONTENT[CONF_TABLE_NM]['select_fields_query']
    CONF_TGT_CNT_SQL = JSON_CONTENT[CONF_TABLE_NM]['tgt_cnt_query']
    CONF_INS_TO_TEMP_TABLE_SQL = JSON_CONTENT[CONF_TABLE_NM].get('insert_to_tmp_table')  
    if CONF_TGT_TBL_NM == 'invstmt_fund_trx' or CONF_TGT_TBL_NM == 'invstmt_fund_asset':
        CONF_NEGATE_CANCEL_UPD_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('negate_cancel_upd').format(CONF_BATCH_DATE)
        CONF_CANCEL_UPD_REVERSAL_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('cancel_and_update_reversal')
        CONF_SECOND_LOAD_UPD_NOMATCH = JSON_CONTENT[CONF_TABLE_NM].get('second_load_upd_nomatch')
        CONF_LATEST_TGT_REC_NEGATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('latest_tgt_rec_negate')
        CONF_SECOND_LOAD_UPD_MATCH = JSON_CONTENT[CONF_TABLE_NM].get('second_load_upd_match')
        CONF_DELETE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('delete_query').format(CONF_BATCH_ID)
    if CONF_TGT_TBL_NM == 'plcy_trx_sumry':
        CONF_SRC_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('src_query')
    if CONF_FILE_TYPE.lower()=='full' and CONF_TYPE ==2:
        CONF_FULL_FILE_INACTIVE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('full_file_inactive_qry')

    BATCH_DATE = CONF_BATCH_DATE
    # if CONF_TGT_TBL_NK :
    #     drop_dup_on_cols = CONF_TGT_TBL_NK.split(',')
    unpersist_dfs =[]  
    unpersist_lkp_dfs =[]
   
    src_cnt, src_fltr_cnt = read_source_file(CONF_SRC_FILE_PATH,CONF_FILE_DELIMITER)
    print(f"Source file data read successfully")

    if src_cnt == 0 and CONF_TGT_TBL_NM !='plcy_trx_sumry':
        print("No data from source !!!")
    elif src_fltr_cnt==src_cnt and CONF_TGT_TBL_NM !='plcy_trx_sumry':
        print("All src data is filtered out!!!")
        fltr_cnt = src_fltr_cnt
    else :
        # Checking if correct type is passed and truncating temp table in case of type1 table
        CONF_TRUNCATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('truncate_query')
        if CONF_TRUNCATE_QUERY:
            TRUNCATE_QUERY = CONF_TRUNCATE_QUERY.format(CONF_SCHEMA,CONF_JOB_NAME)
            pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, TRUNCATE_QUERY, CONF_CERT_FILE, CONF_BUCKETNAME, CONF_REGION)
            print("Truncate query exeution completed ! ")
            print(TRUNCATE_QUERY)
           
        if CONF_TYPE == 0:
            pass
        elif CONF_TYPE == 1:
            CONF_IDENTIFR_QUERY = JSON_CONTENT[CONF_TABLE_NM]['identifier_query']
            if CONF_TGT_TBL_NM != 'invstmt_fund_trx':
                TMP_CONF_UPDATE_QUERY = JSON_CONTENT[CONF_TABLE_NM].get('update_query')
                CONF_UPDATE_QUERY = TMP_CONF_UPDATE_QUERY.format(CONF_SCHEMA,CONF_JOB_NAME)
                pass
        elif CONF_TYPE == 2:
            CONF_IDENTIFR_QUERY = JSON_CONTENT[CONF_TABLE_NM]['identifier_query']
            CONF_SCD_2_UPDATE = JSON_CONTENT[CONF_TABLE_NM]['scd_2_update']
            SCD_2_UPDATE_QUERY = CONF_SCD_2_UPDATE.format(CONF_SCHEMA , CONF_JOB_NAME,CONF_BATCH_DATE,CONF_BATCH_ID)
            CONF_ENTITY_TYPE = JSON_CONTENT[CONF_TABLE_NM].get('entity_type')
            CONF_ENTITY_JOB_NAME = CONF_JOB_NAME +'-'+CONF_ENTITY_TYPE

            RESTART_DELETE = JSON_CONTENT[CONF_TABLE_NM].get('restart_delete')
            RESTART_UPDATE = JSON_CONTENT[CONF_TABLE_NM].get('restart_update')
            if RESTART_DELETE and RESTART_UPDATE :
                print("custom delete and update run ")
                CONF_RESTART_DELETE = RESTART_DELETE.format(CONF_BATCH_ID,CONF_JOB_NAME)
                CONF_RESTART_UPDATE = RESTART_UPDATE.format(CONF_BATCH_DATE,CONF_JOB_NAME)
                print(CONF_RESTART_DELETE)
                print("delete start")
                pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, CONF_RESTART_DELETE, CONF_CERT_FILE,CONF_BUCKETNAME, CONF_REGION)
                print(CONF_RESTART_UPDATE)
                print("delete end and update start ")
                pg_insert(DBUSER, DBHOST, DBPORT, DBNAME, DBPWD, CONF_RESTART_UPDATE, CONF_CERT_FILE,CONF_BUCKETNAME, CONF_REGION)
                print("update end")
               

            else :
                restart_job('SCD2', CONF_SCHEMA, CONF_TGT_TBL_NM, CONF_SUR_KEY, \
                CONF_BATCH_DATE, CONF_BATCH_ID, CONF_ENTITY_JOB_NAME, DBUSER, DBHOST, DBPORT, \
                DBNAME, DBPWD, CONF_CERT_FILE, CONF_BUCKETNAME, CONF_REGION,CONF_SRC_SYS)
        else :
            raise Exception("Invalid Type ::"+CONF_TYPE+" !!!")

        # Creating DFs for target table and applying transformation
        src_cnt_first, fltr_cnt_first, rej_cnt_first, tgt_cnt_first = load_ods_table()
        for df in unpersist_dfs:
            df.unpersist()
        if (CONF_TGT_TBL_NM == 'invstmt_fund_trx' and CONF_FILE_TYPE.lower() == 'time_trade') or (CONF_TGT_TBL_NM == 'invstmt_fund_asset' and CONF_FILE_TYPE.lower() == 'time_asset'):
            print("second load started")
            src_cnt_final, fltr_cnt_final, rej_cnt_final, tgt_cnt_final = load_ods_table(load_type='second_load')
            src_cnt = src_cnt_final
            fltr_cnt = fltr_cnt_first + fltr_cnt_final
            rej_cnt = rej_cnt_first + rej_cnt_final
            tgt_cnt = tgt_cnt_final
 
    audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
    CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
    src_cnt, tgt_cnt, fltr_cnt, rej_cnt, start_time, CONF_RUN_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
    print("Audit entry successful!")

    print("Job completed !!!")

except Exception as e:
    #audit sumry entry
    PrintException()
    audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
    CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
    src_cnt, 0, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
    print("Audit entry successful!")
    sys.exit(e)

job.commit()_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
    src_cnt, tgt_cnt, fltr_cnt, rej_cnt, start_time, CONF_RUN_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
    print("Audit entry successful!")

    print("Job completed !!!")

except Exception as e:
    #audit sumry entry
    PrintException()
    audit_sumry(glueContext, DB_URL, DBUSER, DBPWD, CONF_ODS_CAT_DB_NAME, \
    CONF_ODS_AUD_CAT_TB_NAME, CONF_BATCH_ID, CONF_JOB_NAME, str(CONF_SRC_SYS), \
    src_cnt, 0, fltr_cnt, rej_cnt, start_time, CONF_FAIL_STAT,CONF_TGT_TBL_NM,CONF_SRC_FILE_NAME)
    print("Audit entry successful!")
    sys.exit(e)

job.commit()
