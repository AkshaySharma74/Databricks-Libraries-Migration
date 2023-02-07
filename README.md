## LibraryExport

This is a Databricks Notebook Utility in Python to export Libraries either from a given list of clusters or all clusters present in the workspace to a filesystem location : DBFS/ADLS/S3

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

<img width="858" alt="image" src="https://user-images.githubusercontent.com/91735853/217284737-11bb685d-30c9-41b5-a846-f423e3f425e0.png">


This JSON file contains the information of all the libraries exported for the given cluster and will be utilised while importing the libraries to another cluster.
