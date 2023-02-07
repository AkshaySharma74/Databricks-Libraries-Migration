## LibraryExport

This is a Databricks Notebook Utility in Python to export Libraries either from a given list of clusters or all clusters present in the workspace to a filesystem location : DBFS/ADLS/S3

### How it works?

Following variables needs to be set:
1.EXPORT_LOG_TABLE : Delta Table for storing logs of clusters whose libraries are exported
2.LIBRARY_EXPORT_PATH : Location where exported Libraries will be stored
3.CLUSTER_NAMES : name of Clusters whose libraries needs to be exported, Leave blank if export needs to done for all clusters 

