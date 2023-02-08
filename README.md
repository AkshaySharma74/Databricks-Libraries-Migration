## LibraryExport

This is a Databricks Notebook Utility in Python to export Libraries either from a given list of clusters or all clusters present in the workspace to a filesystem location : DBFS/ADLS/S3. It also maintains date wise versioning so while importing later, libraries exported of any given cluster for a specific date can be imported.

### How it works?

Following variables needs to be set:

> 1. *EXPORT_LOG_TABLE* : Delta Table for storing logs of clusters whose libraries are exported

> 2. *LIBRARY_EXPORT_PATH* : Location where exported Libraries will be stored

> 3. *CLUSTER_NAMES* : name of Clusters whose libraries needs to be exported, leave blank if export needs to done for all clusters 



<img width="836" alt="image" src="https://user-images.githubusercontent.com/91735853/217279665-9b4a277a-77a8-4fb1-bbcb-2f1d1c4f589f.png">

All pre-requisites are done now, All we need to do is create an object of *ExportJars* class and call the *export_cluster_libraries* method using the object.

<img width="503" alt="image" src="https://user-images.githubusercontent.com/91735853/217280968-a03f118f-0605-4f23-8e95-9896bd5f2a2c.png">

Once the processing is done you can validate the export metrics in the table mentioned in *EXPORT_LOG_TABLE*:

<img width="1611" alt="image" src="https://user-images.githubusercontent.com/91735853/217282779-21f2761a-b680-4f62-a914-03c6bdf9de15.png">

On the FileSystem you will be able to see the the Libraries exported along with CLUSTER_NAME.json file.

![image](https://user-images.githubusercontent.com/91735853/217490755-b47fd740-af96-420f-90c7-583a0282ab16.png)


This JSON file contains the information of all the libraries exported for the given cluster and will be utilised while importing the libraries to another cluster.



## LibraryImport

This is a Databricks Notebook Utility in Python to import Libraries from a cluster previous exported to a filesystem location to our target cluster.


### How it works?

Following variables needs to be set:

> 1. *BASE_PATH* : Location where Libraries were exported, same libraries will be imported from within this location

> 2. *SOURCE_CLUSTER* : Libraries of the cluster that needs to be imported 

> 3. *TARGET_CLUSTER* : Name of the cluster where libraries of source cluster needs to be installed

> 4. *LOG_TABLE_NAME* : Delta Table for storing logs of clusters where libraries were imported

> 5. *IMPORT_DATE* : The version of libraries that needs to be imported

![image](https://user-images.githubusercontent.com/91735853/217492168-538e5a53-115c-430f-ae91-f0f98168c7dd.png)



All pre-requisites are done now, All we need to do is create an object of *ImportJars* class and call the *import_libraries_to_cluster* method using the object.

<img width="503" alt="image" src="https://user-images.githubusercontent.com/91735853/217280968-a03f118f-0605-4f23-8e95-9896bd5f2a2c.png">

Once the processing is done you can validate the export metrics in the table mentioned in *EXPORT_LOG_TABLE*:

<img width="1611" alt="image" src="https://user-images.githubusercontent.com/91735853/217282779-21f2761a-b680-4f62-a914-03c6bdf9de15.png">

On the FileSystem you will be able to see the the Libraries exported along with CLUSTER_NAME.json file.

![image](https://user-images.githubusercontent.com/91735853/217490510-30b8af67-921c-44f2-9dfb-da313ae0fe1f.png)


This JSON file contains the information of all the libraries exported for the given cluster and will be utilised while importing the libraries to another cluster.
