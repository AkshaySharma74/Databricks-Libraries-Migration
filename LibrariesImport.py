# Databricks notebook source
import logging
import sys
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.clusters.api import ClusterApi
from pyspark.sql.functions import from_json, col
from databricks_cli.libraries.api import LibrariesApi
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import time
from datetime import datetime
import uuid

# COMMAND ----------

BASE_PATH = "/FileStore/table/AKSHAY_SHARMA_LIBS" #Libraies will be imported from this dir
SOURCE_CLUSTER = "Shared Autoscaling Americas"
TARGET_CLUSTER = "Shared Autoscaling EMEA (clone)"
LOG_TABLE_NAME = "default.akshay_log_test"
IMPORT_DATE = "2023-02-08"  

# COMMAND ----------

class ImportJars():
  
  """class for exporting jars"""
  
  logging.basicConfig(format='%(asctime)s  %(message)s')
  logger = logging.getLogger("driver_logger")
  logger.setLevel(logging.INFO)
  
  def __get_logs_schema(self):
    return StructType(
    [
      StructField("unique_identifier", StringType(), False),
      StructField("record_timestamp", TimestampType(), nullable = False),
      StructField("source_cluster", StringType(), True),
      StructField("target_cluster", StringType(), True),
      StructField("import_date", DateType(), True),
      StructField("path", StringType(), True),
      StructField("type", StringType(), True),
      StructField("message", StringType(), True),
      StructField("load_id", TimestampType(), True),
      StructField("action", StringType(), True)
    ]
    )
  
  def __init__(self,root_path,delta_logging=False,table_name=None,token =None,host=None):
    if token is None:
      token = dbutils.entry_point.getDbutils().notebook().getContext().apiToken().get()
    if host is None:
      host = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    self.apiclient = ApiClient(token = token,host = host)
    self.lib_api = LibrariesApi(self.apiclient)
    self.cluster_api = ClusterApi(self.apiclient)
    self.root_path = root_path
    self.log_table = table_name
    self.delta_logging = delta_logging
    self.load_id = datetime.strptime(datetime.utcnow().strftime('%Y-%m-%d-%H:%M:%S'),'%Y-%m-%d-%H:%M:%S')
    self.__source_cluster_name = None
    self.__target_cluster_name = None
    self.__import_date = None
    self.__msg_orders = []
    
  
  def __set_class_variables(self,source_cluster_name,target_cluster_name,import_date):
       self.__source_cluster_name = source_cluster_name
       self.__target_cluster_name = target_cluster_name
       self.__import_date = import_date
      
      
  def __log_to_table(self):
       spark.createDataFrame(
         self.__msg_orders,
        self.__get_logs_schema()
      ).select("unique_identifier",
               "record_timestamp",
               "source_cluster","target_cluster",
               to_date(F.col("import_date"),"yyyy-MM-dd").alias("import_date"),
               "path","type",
               "message","load_id","action"
              ).orderBy("record_timestamp").write.option("schema",self.__get_logs_schema()).format("delta").mode("append").saveAsTable(self.log_table)
      
           
  def __status_logger(self,message):
      """single method for logging"""
      self.logger.info(message)
      if self.delta_logging:
        time = datetime.strptime(datetime.utcnow().strftime('%Y-%m-%d-%H:%M:%S.%f'),'%Y-%m-%d-%H:%M:%S.%f')
        source = self.__source_cluster_name
        target = self.__target_cluster_name
        if self.__import_date:
          date = datetime.strptime(self.__import_date,'%Y-%m-%d')
        else:
          date = datetime.strptime(datetime.utcnow().strftime('%Y-%m-%d'),'%Y-%m-%d')
        unique_identifier = uuid.uuid4().hex
        activity = message
        source_path = None
        source_type = None
        action = "IMPORT"
        if "IMPORT_LOCATION" in message:
          source_path = message.split("IMPORT_LOCATION")[1].split("->")[1].replace(":","").strip()
          source_type = message.split("IMPORT_LOCATION")[1].split("->")[0].replace(":","").strip()
        self.__msg_orders.append((unique_identifier,time,source,target,date,source_path,source_type,activity.rstrip(),self.load_id,action))

  
  def __print_lib_status(self,success,failed,total_libs_count):
      """Print how many have succeded and failed out of total libraries"""
      self.__status_logger(str(success)+"/"+str(total_libs_count)+" have installed")
      self.__status_logger(str(failed)+"/"+str(total_libs_count)+" have failed\n")    
        
    
  def __check_status(self,cluster_id):

      """Check every 10 secs whether jars have installed"""
      install_complete_flag = False
      total_libs_count = len(self.lib_api.cluster_status(cluster_id)['library_statuses'])
      while not install_complete_flag:
        success = 0 
        failed  = 0
        others = 0
        for lib in self.lib_api.cluster_status(cluster_id)['library_statuses']:
          if lib['status'].lower() == 'pending':
            pass
          elif lib['status'].lower() == 'installed':
            success = success+1
          elif lib['status'].lower() == 'failed':
            failed = failed+1
          else:
            others= others+1
        self.__print_lib_status(success,failed,total_libs_count-others)
        
        if(success+failed == total_libs_count-others):
          install_complete_flag = True
          self.__status_logger("COMPLETED")
          self.__print_lib_status(success,failed,total_libs_count-others)  
          break
        time.sleep(10)  

  def import_libraries_to_cluster(self,source_cluster_name,target_cluster_name,import_date):

      """install libraries from a dbfs location to cluster"""

      self.__set_class_variables(source_cluster_name,target_cluster_name,import_date)
      self.__status_logger("INSTALLATION STARTED\n")
      cluster_json = self.__import_json_from_cluster_id(source_cluster_name,import_date)
      libs = self.__get_exported_jars(cluster_json,source_cluster_name,import_date)
      total_libs_count = len(libs)
      self.lib_api.install_libraries(self.cluster_api.get_cluster_id_for_name(target_cluster_name),libs) #target Cluster needs to be up
      self.__check_status(self.cluster_api.get_cluster_id_for_name(target_cluster_name))
      self.__status_logger("Import Activity Finished")
      if self.delta_logging:
        self.__log_to_table()
        
     
  def install_libraries_to_cluster(self,libs,target_cluster_name):
    installation_libs = []
    for i in libs:
      if list(i.keys())[0].strip().lower() == "maven":
        installation_libs.append({list(i.keys())[0].strip().lower():{"coordinates": i[list(i.keys())[0]]}})
      elif list(i.keys())[0].strip().lower() in ["pypi","cran"]:
        installation_libs.append({list(i.keys())[0].strip().lower():{"package": i[list(i.keys())[0]]}})
      else:
        installation_libs.append(i)
    print(installation_libs)    
    self.lib_api.install_libraries(self.cluster_api.get_cluster_id_for_name(target_cluster_name),installation_libs)
    self.__check_status(self.cluster_api.get_cluster_id_for_name(target_cluster_name))
    self.__status_logger("Import Activity Finished")
    if self.delta_logging:
        self.__log_to_table()
  

  def __get_exported_jars(self,cluster_json,cluster_name,import_date):

      """Get jars that needs to be installed"""
      libs = []
      if cluster_json.get('library_statuses'):
        for library in cluster_json['library_statuses']:
          if library['library'] is not None:
            lib_key = list(library['library'].keys())[0]
            lib_value = list(library['library'].values())[0]
            if isinstance(lib_value,str) and (lib_value.startswith("dbfs") or lib_value.startswith("abfss") or lib_value.startswith("s3")):
              lib_value_split = lib_value.split("/")
              libs.append({lib_key:lib_value.split(":",1)[0]+":"+self.root_path+"/"+cluster_name.replace(" ","_")+"/"+import_date+"/"+lib_value_split[-1].replace(" ","_")})
              self.__status_logger("IMPORT_LOCATION : "+lib_key+"->"+lib_value.split(":",1)[0]+":"+self.root_path+"/"+cluster_name.replace(" ","_")+"/"+import_date+"/"+lib_value_split[-1].replace(" ","_"))
            else:
              libs.append(library['library'])
              self.__status_logger("IMPORT_LOCATION : "+"REMOTE_REPO"+"->"+json.dumps(library['library']))
      return libs 

  def __import_json_from_cluster_id(self,cluster_name,import_date):

    """Read json file for cluster_id"""

    with open("/dbfs"+self.root_path+"/"+cluster_name.replace(" ","_")+"/"+import_date+"/"+cluster_name.replace(" ","_")+'.json') as f:
      json_data = json.load(f)
      return json_data

    

# COMMAND ----------

obj = ImportJars(BASE_PATH,True,LOG_TABLE_NAME)  

# COMMAND ----------

# obj.import_libraries_to_cluster(SOURCE_CLUSTER,TARGET_CLUSTER,IMPORT_DATE) #ENTRY POINT #param1 is for jars of source cluster, param2 is target cluster

# COMMAND ----------

libs = [ \
  {"jar":"dbfs:/FileStore/table/AKSHAY_SHARMA_LIBS/Shared_Autoscaling_Americas/2023-02-08/6d22271f_a3a0_4a75_adae_2905ab235f59-spark_xml_2_12_0_14_0-b25fb.jar"},
  {"maven" : "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.0.0"},
  {"pypi":"dbt-databricks>=1.0.0,<2.0.0"},
  {"whl":"dbfs:/FileStore/table/AKSHAY_SHARMA_LIBS/Shared_Autoscaling_Americas/2023-02-08/deltaoptimizer-1.0.0-py3-none-any.whl"},
  {"cran":"shinyjs"}
  
]

obj.install_libraries_to_cluster(libs,"Akshay Sharma's Cluster")
