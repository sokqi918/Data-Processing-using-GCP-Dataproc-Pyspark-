# Data-Processing-using-GCP-Dataproc-Pyspark-
This repository provide a basic idea of data processing by using Pyspark in GCP Dataproc.

![Updated Image](https://github.com/sokqi918/Data-Processing-using-GCP-Dataproc-Pyspark-/blob/main/Photo/Screenshot%202024-02-04%20020140.jpg)

Google Cloud Platform's range of data processing tools includes Cloud Dataflow and Cloud Dataproc, with the latter chosen for our specific needs due to its compatibility with Apache Spark and Hadoop, fully managed service structure, seamless integration with Cloud Storage, and cost-effectiveness. Unlike Cloud Dataflow, which targets stream and batch processing, Dataproc specializes in running Spark and Hadoop clusters, offering a swift solution for large-scale data processing and analytics, ideal for handling our daily dataset retrievals and substantial data volumes. Preferred for scalable, well-managed, and optimized big data processing, Dataproc's flexibility is advantageous, particularly for developing Python-based Spark applications using PySpark. Its seamless integration with Cloud Storage streamlines data transfer and access, minimizing the need for extensive data movement, making it an optimal choice for our dataset's destination in Cloud Storage.

### Process Flow: 
The implementation of the data processing part involves using Google Cloud Dataproc to manage Spark and Hadoop services and submitting a Pyspark job to perform specific data processing tasks. Below is a step-by-step outline of the data processing implementation: 

**1. Dataproc** 
Setup Spark and Hadoop Cluster – create a datarpoc cluster with the necessary Spark and Hadoop configurations to handle our data processing tasks. “cluster-dataprocessing” has been created in Dataproc. 
![Updated Image](https://github.com/sokqi918/Data-Processing-using-GCP-Dataproc-Pyspark-/blob/main/Photo/dataproc.jpg)
