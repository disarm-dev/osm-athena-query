/**
* DiSARMOSMHandler - Service handler
* 
* @author  Vikram Sridharan
* @version 1.0
* @since   2018-10-15 
*/

package com.ucsf.disarm.lambda;

import com.amazonaws.services.athena.AmazonAthena;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.LambdaLogger;


import org.json.simple.JSONObject;
//import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import com.google.maps.model.LatLng;
import com.google.maps.android.SphericalUtil;

public class DiSARMOSMHandler implements RequestStreamHandler {
    JSONParser parser = new JSONParser();


    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        LambdaLogger logger = context.getLogger();
        logger.log("Loading Lambda handler for DiSARMOSMHandler");


        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONObject responseJson = new JSONObject();
        
        String min_latitude = null;
        String max_latitude = null;
        
        String min_longitude = null;
        String max_longitude = null;
        
        //String query_service = "osm_query";
        String query_service = null;
        String responseCode = "200";
        
        JSONObject responseBody = null;

        try {
            JSONObject event = (JSONObject)parser.parse(reader);
            
            if (event.get("pathParameters") != null) 
            {
                JSONObject pps = (JSONObject)event.get("pathParameters");
                if ( pps.get("proxy") != null) 
                {
                	query_service = (String)pps.get("proxy");
                	if(!query_service.equalsIgnoreCase("osm_query"))
                		throw new ParseException(2);                		
                }else
                {
                	throw new ParseException(2);
                }
            }else
            {
            	throw new ParseException(2);
            }
            
            
            if (event.get("queryStringParameters") != null) 
            {
                JSONObject qps = (JSONObject)event.get("queryStringParameters");
                if ( qps.get("min_latitude") != null) 
                {
                	min_latitude = (String)qps.get("min_latitude");
                	logger.log("min_latitude" + min_latitude);
                } else
                {
                	throw new ParseException(2);
                }
                
                if ( qps.get("max_latitude") != null)
                {
                	max_latitude = (String)qps.get("max_latitude");
                } else
                {
                	throw new ParseException(2);
                }
                
                if ( qps.get("min_longitude") != null) 
                {
                	min_longitude = (String)qps.get("min_longitude");
                } else
                {
                	throw new ParseException(2);
                }
                
                if ( qps.get("max_longitude") != null) 
                {
                	max_longitude = (String)qps.get("max_longitude");
                } else
                {
                	throw new ParseException(2);
                }               
                
            }else
            {
            	logger.log("Missing query parameters.");
            	throw new ParseException(2);
            } 
            
            //compute closed path
            /*
            List path = new ArrayList(4);
            
            LatLng p1 = new LatLng(Double.valueOf(min_latitude), Double.valueOf(min_longitude));
            
            LatLng p3 = new LatLng(Double.valueOf(max_latitude), Double.valueOf(max_longitude));
            
            LatLng p2 = new LatLng(Double.valueOf(max_latitude), Double.valueOf(min_longitude));
            
            LatLng p4 = new LatLng(Double.valueOf(min_latitude), Double.valueOf(max_longitude));
            
            path.add(p1);
            path.add(p2);
            path.add(p3);
            path.add(p4);
            path.add(p1);
            
            double spherical_area = SphericalUtil.computeArea(path)/1000000;
            
            logger.log("computed area for bbox in sq. kms");
            logger.log(Double.toString(spherical_area));
            */
            
            // Build an AmazonAthena client
            DiSARMAthenaClientFactory factory = new DiSARMAthenaClientFactory();
            AmazonAthena client = factory.createClient();

            DiSARMOSMQueryHandler.setBBoxParams(min_latitude, max_latitude, min_longitude, max_longitude);
            
            logger.log("bbbox params");
            logger.log(min_latitude + " " + max_latitude + " " + min_longitude + " " + max_longitude);
            
            logger.log("Query String");
            logger.log(DiSARMOSMQueryHandler.getQueryStringForAllBldgs());
            
            String queryExecutionId = DiSARMOSMQueryHandler.submitAthenaQuery(client);
            
            //String queryExecutionId = "1234555djkdkkllkdkl";
            
            logger.log("query execution id");
            logger.log(queryExecutionId);
            
            responseBody = new JSONObject();
            responseBody.put("message", "Transaction accepted. Submitted for async processing.");
            responseBody.put("aws_query_execution_id", queryExecutionId);
            
            responseJson.put("isBase64Encoded", false);
            responseJson.put("statusCode", responseCode);
            responseJson.put("headers", null);            
            responseJson.put("body", responseBody.toString());  

        } catch(ParseException pex) {
        	responseJson = new JSONObject();
        	responseBody = new JSONObject();
        	if(min_latitude == null || max_latitude == null ||
        			min_longitude == null || max_longitude == null)
        	{
        		responseBody.put("error_missing_values", "Missing values in bounding box.");
        		responseBody.put("min_latitude", min_latitude);
        		responseBody.put("max_latitude", max_latitude);
        		responseBody.put("min_longitude", min_longitude);
        		responseBody.put("max_longitude", max_longitude);
        	}
        	if(query_service == null || !query_service.equalsIgnoreCase("osm_query"))
        	{
        		logger.log("Query Service: " + query_service);
        		responseBody.put("error_missing_service", "Missing or unrecognized service.");
        	}        	
        	//responseBody.put("exception", pex);
        	
        	responseJson.put("isBase64Encoded", false);
            responseJson.put("statusCode", "400");
            responseJson.put("headers", null);   
            responseJson.put("body", responseBody.toString()); 
            //responseJson.put("exception", pex);
            
        }
        logger.log(responseJson.toJSONString());
        OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
        writer.write(responseJson.toJSONString());  
        writer.close();
    }
}