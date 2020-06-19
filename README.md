# osm-athena-query
 
 ## Introduction
 
OpenStreetMap (OSM) is a free, editable map of the world, created and maintained by volunteers and available for use under an open license. OSM can be used to provide maps, directions, and geographic context to users around the world. Users join the open mapping community, more and more valuable data is being added to OpenStreetMap, requiring increasingly powerful tools, interfaces, and approaches to explore it. Through the Amazon Web Services (AWS) Public Datasets program, regular OSM data archives are made available in Amazon S3 in a few different formats (ORC, PBF).

The osm-athena-query is a prototype for an interface to query publicly available OSM data stored on Amazon S3 using Amazon Athena. Amazon Athena is a serverless, interactive query service to query data and analyze big data in Amazon S3 using standard SQL.

To satisfy DiSARM's need to efficiently query building structures within specific geographic bounds, the *osm-athena-query* service provides an efficient method to query and retrieve this data.

## R Client Interface

*osm-athena-query/R/disarm_osm_aws_services.R* - is a R client interface to query OSM data on Amazon S3. The client interface currently provides the following functionality:

   *disarm_aws_service*: function accepts a bounding box and sends the query to the DiSARM OSM service on AWS.
    Upon successful submission of request to AWS the function returns a AWS query id. AWS will place the results on 
    Amazon S3 when complete. If transaction is not successful a status_code other than 200 is returned with an error 
    message.
    
  *disarm_aws_osm_results*: function accepts the AWS query id's for retrieval of the results from Amazon S3.
   The results files (formatted as csv) are pulled down to the current working directory of R.

## Server-side Service

*osm-athena-query/src/main/java/com/ucsf/disarm/lambda/* - contains Java code that accepts and validates client side parameters and processes the retrieval request.

*osm-athena-query/src/main/java/com/ucsf/disarm/lambda/DiSARMOSMHandler.java* - is the AWS Lambda Service that accepts the client requests for validation and submission of the query. Once the query parameters provided are satisfied, the service build an Amazon Athena client and facilitates submission of the query for execution. Once the query has been submitted the submitted queryid token is returned to the caller. The results of the query are placed on Amazon S3 and the client will use the query-id to retrieve the results.

*osm-athena-query/src/main/java/com/ucsf/disarm/lambda/DiSARMAthenaClientFactory.java* - generates Amazon Athena client stubs to service incoming queries.

*osm-athena-query/src/main/java/com/ucsf/disarm/lambda/DiSARMOSMQueryHandler.java* - builds the query search string and invokes the query submission.


