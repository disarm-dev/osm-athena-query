#Author: Vikram Sridharan
#Description: R functions to access OSM query service on AWS 
#Services offered:
# a. disarm_aws_service: function accepts a bounding box and sends the query to the DiSARM OSM service on AWS.
#    Upon successful submission of request to AWS the function returns a AWS query id. AWS will place the results on 
#    Amazon S3 when complete. If transaction is not successful a status_code other than 200 is returned with an error 
#    message.
# b. disarm_aws_osm_results: function accepts the AWS query id's for retrieval of the results from Amazon S3.
#    The results files (formatted as csv) are pulled down to the current working directory of R.
#############################################################################################################################
# Example:                                                                                                                  #
#############################################################################################################################
# source("disarm_osm_aws_services.R")
# min_lat_long <- c(-26.6801441414,31.7082569399)
# max_lat_long <- c(-26.4049492727,32.0506640547)
# submit_query_to_aws <- disarm_aws_service(min_lat_long, max_lat_long, enforce_max_sq_kms = TRUE)
# AWS queries have a timeout of 30 minutes. for the bounding box in this example the area is 1,040 sq. kms.
# this query takes about 2.5 minutes to complete. when enforce_max_sq_kms = TRUE, the current max area bounds, 
# which is set to 1,500 sq. kms., is enforced. when enforce_max_sq_kms = FALSE the check is by-passed.
# the function caller needs to take this upper limit into account. implementing a reasonable max sq. kms. is
# a prudent idea.
# status_code(submit_query_to_aws) - check that its 200 (for success). if failure retrieve the appropriate http stataus code
# content(submit_query_to_aws) - retrieve aws query id and messages. if failure error message. 
# example aws query id: b2fe614e-952d-4bda-aed9-d06cfe77a45d
# query is asynchronous. depending on the query it may take a while to complete and deposit results to amazon s3.
# save_file <- disarm_aws_osm_results(c("b2fe614e-952d-4bda-aed9-d06cfe77a45d"))
# function return TRUE. the results file is deposited to the current working directory.
# in this example its saved as ./b2fe614e-952d-4bda-aed9-d06cfe77a45d.csv

library(httr)
library(aws.signature)
library(aws.s3)
library(geosphere)
library(stringr)

Sys.setenv("AWS_ACCESS_KEY_ID" = "AKIAI3KSYT5ANVSFS3FQ",
           "AWS_SECRET_ACCESS_KEY" = "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",
           "AWS_DEFAULT_REGION" = "us-east-1")

disarm_aws_service <- function(min_lat_long, max_lat_long, enforce_max_sq_kms = TRUE)
{
  if(length(min_lat_long) != 2 || length(max_lat_long) != 2)
  {
    stop("Please provide function with two vectors with first containing the lat and longitude of 
         lower bounds and the second upper bounds of the bounding box.
         usage: disarm_aws_service(c(min_lat,min_long), c(max_lat, max_long))")
  }
  
  p1lat <- min_lat_long[1]
  p1long <- min_lat_long[2]
    
  p2lat <- max_lat_long[1]
  p2long <- min_lat_long[2]
  
  p3lat <- max_lat_long[1]
  p3long <- max_lat_long[2]
  
  p4lat <- min_lat_long[1]
  p4long <- max_lat_long[2]
  
  lat_long_polygon_frame <- data.frame(longitude = c(p1long, p2long, p3long, p4long, p1long),
                                       latitude = c(p1lat, p2lat, p3lat, p4lat, p1lat))
  lat_long_polygon_area <- areaPolygon(lat_long_polygon_frame)/1000000
  
  if(enforce_max_sq_kms && lat_long_polygon_area >= 1500)
  {
    stop("Please provide a bounding box that is less than or equal to 1,500 sq.kms. Current 
         area: ", round(lat_long_polygon_area, 2))
  }
    
  get_url <- "https://ce18q5eeh9.execute-api.us-east-1.amazonaws.com/development/osm_query"
  
  params <- list("min_latitude" = min_lat_long[1], "max_latitude" = max_lat_long[1], 
                 "min_longitude" = min_lat_long[2], "max_longitude" = max_lat_long[2])
  
  auth_header_name <- "X-Api-Key"
  auth_header_value <- "3v3WBotj5NaMaeXx2EuG39utUZ4IqXm07a0DnK9z"
  names(auth_header_value) <- auth_header_name
  
  aws_call_results <- GET(get_url, query = params, add_headers(.headers = auth_header_value))
  
  aws_call_results
}

disarm_aws_osm_results <- function(query_ids)
{
  aws_results_bucketname <- 'disarm-query-results'
  
  buckets <- get_bucket_df(
    bucket = aws_results_bucketname,
    key = "AKIAI3KSYT5ANVSFS3FQ",
    secret = "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"
  )
  
  required_files <- vector()
  
  for(j in seq_along(query_ids))
  {
    for(i in 1:nrow(buckets))
    {
      if(str_detect(buckets[i,]$Key, query_ids[j]))
      {
        required_files <- c(required_files, buckets[i,]$Key)
      }
    }
    
  }
  
  for(o in seq_along(required_files))
  {
    if(!str_detect(required_files[o], ".metadata"))
    {
        saved_file <- save_object(required_files[o], "disarm-query-results")
        if(!str_detect(saved_file, required_files[o]))
        {
            warning_message <- paste("Unable to download file associated with AWS queryid: ",  
                                  str_sub(saved_file, 1, str_locate(saved_file, ".csv")[1,1] - 1))
            warning(warning_message)
        }
      
    }else
    {
        deleted_file <- delete_object(required_files[o], "disarm-query-results")
        if(!deleted_file)
        {
            warning_message <- paste("Unable to delete meta file on AWS S3. Manual deletion adviced. queryid: ",  
                                     str_sub(required_files[o], 1, str_locate(required_files[o], ".csv")[1,1] - 1))
            warning(warning_message)
        }
    }
  }
  
  return(TRUE)
  
}
  