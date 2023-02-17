# Databricks notebook source
import logging
import sys
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.clusters.api import ClusterApi
from pyspark.sql.functions import from_json, col
from databricks_cli.libraries.api import LibrariesApi
from pyspark.sql import functions as F
import json
from datetime import datetime
import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

EXPORT_LOG_TABLE = "default.akshay_log_test3"
LIBRARY_EXPORT_PATH = "/FileStore/table/AKSHAY_SHARMA_LIBS" #LIBS WILL BE SAVED WITHIN THIS DIR
CLUSTER_NAMES = [] #PASS A LIST OF CLUSTERS OR LEAVE IT BLANK FOR ALL CLUSTERS IN WORKSPACE

# COMMAND ----------

class ExportJars():
  
  """class for exporting jars"""
  
  logging.basicConfig(format='%(asctime)s  %(message)s')
  logger = logging.getLogger("driver_logger")
  logger.setLevel(logging.INFO)
    
  def __init__(self,root_path,delta_logging=False,table_name=None,token =None,host=None):
    if token is None:
      token = dbutils.entry_point.getDbutils().notebook().getContext().apiToken().get()
    if host is None:
      host = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    self.apiclient = ApiClient(token = token,host = host)
    self.lib_api = LibrariesApi(self.apiclient)
    self.cluster_api = ClusterApi(self.apiclient)
    self.root_path = root_path
    self.todays_date = datetime.utcnow().strftime('%Y-%m-%d')
    self.log_table = table_name
    self.delta_logging = delta_logging
    self.load_id = datetime.strptime(datetime.utcnow().strftime('%Y-%m-%d-%H:%M:%S'),'%Y-%m-%d-%H:%M:%S')
    self.__msg_orders = []
    
    
  def __get_logs_schema(self):
    return StructType(
    [
      StructField("unique_identifier", StringType(), False),
      StructField("record_timestamp", TimestampType(), nullable = False),
      StructField("source_cluster", StringType(), True),
      StructField("target_cluster", StringType(), True),
      StructField("export_date", DateType(), True),
      StructField("path", StringType(), True),
      StructField("type", StringType(), True),
      StructField("message", StringType(), True),
      StructField("load_id", TimestampType(), True),
      StructField("action", StringType(), True)
    ]
    )
  
    
  def __log_to_table(self):
       spark.createDataFrame(
         self.__msg_orders,
        self.__get_logs_schema()
      ).select("unique_identifier",
               "record_timestamp",
               "source_cluster","target_cluster",
               to_date(F.col("export_date"),"yyyy-MM-dd").alias("export_date"),
               "path","type",
               "message","load_id","action"
              ).orderBy("record_timestamp").write.option("schema",self.__get_logs_schema()).format("delta").mode("append").saveAsTable(self.log_table)
      
           
  def __status_logger(self,message):
      """single method for logging"""
      self.logger.info(message)
      if self.delta_logging:
        time = datetime.strptime(datetime.utcnow().strftime('%Y-%m-%d-%H:%M:%S.%f'),'%Y-%m-%d-%H:%M:%S.%f')
        source = None
        target = None
        date = datetime.strptime(self.todays_date,'%Y-%m-%d')
        unique_identifier = uuid.uuid4().hex
        activity = message
        source_path = None
        source_type = None
        action = "EXPORT"
        if "->" in message:
          source_path = message.split("->")[1]
          if "JSON_EXPORT" in message:
            source_type = "JSON"
          elif "LIBRARY_EXPORT" in message:
            source_type = "LIBRARY"
        if ":" in message:
          source = message.split(":")[0].strip()
        self.__msg_orders.append((unique_identifier,time,source,target,date,source_path,source_type,activity.rstrip(),self.load_id,action))
        
        
  def __get_clusters_list(self,clusters):

    """get list of all clusters"""
    if clusters:
      return [(self.cluster_api.get_cluster_id_for_name(cluster),cluster) for cluster in clusters]
    else:
      return [(cluster['cluster_id'],cluster['cluster_name']) for cluster in self.cluster_api.list_clusters()['clusters']]


  def export_cluster_libraries(self,clusters=None):

    """Search All the libraries from within the cluster and export to root_path"""
    for cluster in self.__get_clusters_list(clusters):
      cluster_status = self.lib_api.cluster_status(cluster[0])
      self.__status_logger(cluster[1]+" : "+"Export of Libraries Started")
      if cluster_status.get('library_statuses'):
        self.__export_json(cluster[1],cluster_status)
        for library in cluster_status['library_statuses']:
          if(library['status'].lower() != "failed"):
            lib_value = list(library['library'].values())[0]
            lib_key = list(library['library'].keys())[0]
            if isinstance(lib_value,str) and (lib_value.startswith("dbfs") or lib_value.startswith("abfss") or lib_value.startswith("s3")):
              self.__export_library(cluster[1],lib_value)
          else:
            self.__status_logger(cluster[1]+" : "+"Export of Jar Failed with reason:\n"+str(library["messages"])+"\n")
    self.__status_logger("Export of Libraries Finished")
    if self.delta_logging:
        self.__log_to_table()
        
  def __export_library(self,cluster_name,input_path):

    """export library in cluster_id directory for each respective cluster"""

    
    input_path_split = input_path.split("/")
    destination_jar_path = self.root_path+"/"+cluster_name.replace(" ","_")+"/"+self.todays_date+"/"+input_path_split[-1]
    try:
      dbutils.fs.cp(input_path,destination_jar_path)
      self.__status_logger(cluster_name+" : "+"LIBRARY_EXPORT : Library Exported to -> "+destination_jar_path)
    except Exception as e:
      self.__status_logger(cluster_name+" : "+"LIBRARY_EXPORT : Library Export Failed becuase of -> "+str(e))


  def __export_json(self,cluster_name,json_text):

    """Write json file for cluster_id"""
    json_path = self.root_path+"/"+cluster_name.replace(" ","_").replace(" ","_")+"/"+self.todays_date+"/"+cluster_name.replace(" ","_")+".json"
    dbutils.fs.put(json_path,json.dumps(json_text, indent=4),overwrite=True)
    self.__status_logger(cluster_name.replace(" ","_")+" : "+"JSON_EXPORT : Json Exported to -> "+json_path)


# COMMAND ----------



# COMMAND ----------

obj = ExportJars(LIBRARY_EXPORT_PATH,True,EXPORT_LOG_TABLE)
obj.export_cluster_libraries(CLUSTER_NAMES) #ENTRY POINT

# COMMAND ----------

# MAGIC %sh cat /dbfs/FileStore/table/AKSHAY_SHARMA_LIBS/Shared_Autoscaling_Americas/2023-02-17/Shared_Autoscaling_Americas.json

# COMMAND ----------


