/**
 * DiSARMAuqeryHandler to submit a query to Athena for execution, return query handle submission ID.
 *
 *
 * @author  Vikram Sridharan
 * @version 1.0
 * @since   2018-10-15 
 */


package com.ucsf.disarm.lambda;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.athena.model.StartQueryExecutionResult;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class DiSARMOSMQueryHandler 
{
	  static String ATHENA_OSM_DB_NAME = "sampledb";
	  static String ATHENA_OSM_OUTPUT_BUCKET = "s3://disarm-query-results";
	  static String ATHENA_OSM_QUERY_ALL_BUIILDINGS = "WITH nodes_in_bbox AS(" + 
	  		"  SELECT id, lat, lon, type, tags FROM planet" + 
	  		"  WHERE type = 'node'" + 
	  		"  AND lon BETWEEN <min_long> AND <max_long>" + 
	  		"  AND lat BETWEEN <min_lat> AND <max_lat>" + 
	  		" ), " + 
	  		"ways AS(" + 
	  		"  SELECT type, id, tags, nds FROM planet" + 
	  		"  WHERE type = 'way'" + 
	  		" ), " + 
	  		"relation_ways AS(" + 
	  		"  SELECT r.id, r.tags, way.ref, way.role, way_position" + 
	  		"  FROM planet r" + 
	  		"  CROSS JOIN UNNEST(r.members)" + 
	  		"  WITH ORDINALITY AS m (way, way_position)" + 
	  		"  WHERE r.type = 'relation'" + 
	  		"  AND element_at(r.tags, 'type') = 'multipolygon' " + 
	  		"  AND way.role = 'outer' AND way.type = 'way'" + 
	  		" ) " + 
	  		"SELECT w.id AS way_id," + 
	  		" n.id AS node_id," + 
	  		" r.id AS relation_id," + 
	  		" COALESCE(r.id, w.id) AS building_id," + 
	  		" element_at(COALESCE(r.tags, w.tags), 'building') AS building_type," +
	  		" n.lon, n.lat," + 
	  		" node_position," + 
	  		" COALESCE(r.tags['name'], w.tags['name']) AS name" + 
	  		" FROM ways w" + 
	  		" CROSS JOIN UNNEST(w.nds)" + 
	  		" WITH ORDINALITY AS t (nd, node_position)" + 
	  		" JOIN nodes_in_bbox n ON n.id = nd.ref" + 
	  		" LEFT OUTER JOIN relation_ways r ON w.id=r.ref" + 
	  		" WHERE element_at(COALESCE(r.tags, w.tags), 'building') IS NOT NULL" + 
	  		" ORDER BY relation_id, way_position, way_id, node_position;";
	  
	  static String ATHENA_OSM_QUERY_BUIILDINGS_WITH_TAG_YES = "";
	  static String BBOX_MIN_LAT = null;
	  static String BBOX_MAX_LAT = null;
	  static String BBOX_MIN_LONG = null;
	  static String BBOX_MAX_LONG = null;
	  
	  public static void setBBoxParams(String min_lat, String max_lat, String min_long, String max_long)
	  {
		  DiSARMOSMQueryHandler.BBOX_MIN_LAT = min_lat;
		  DiSARMOSMQueryHandler.BBOX_MAX_LAT = max_lat;
		  DiSARMOSMQueryHandler.BBOX_MIN_LONG = min_long;
		  DiSARMOSMQueryHandler.BBOX_MAX_LONG = max_long;
		  
		  DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS = DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS.replaceFirst("<min_lat>", min_lat);
		  DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS = DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS.replaceFirst("<max_lat>", max_lat);
		  DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS = DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS.replaceFirst("<min_long>", min_long);
		  DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS = DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS.replaceFirst("<max_long>", max_long);
	  }
	  
	  public static String getQueryStringForAllBldgs()
	  {
		  return DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS;
	  }

	  /**
	   * Submits a sample query to Athena and returns the execution ID of the query.
	   */
	  public static String submitAthenaQuery(AmazonAthena client)
	  {
	      // The QueryExecutionContext allows us to set the Database.
	      QueryExecutionContext queryExecutionContext = new QueryExecutionContext().withDatabase(DiSARMOSMQueryHandler.ATHENA_OSM_DB_NAME);
	      
	      // The result configuration specifies where the results of the query should go in S3 and encryption options
	      ResultConfiguration resultConfiguration = new ResultConfiguration()
	              // You can provide encryption options for the output that is written.
	              // .withEncryptionConfiguration(encryptionConfiguration)
	              .withOutputLocation(DiSARMOSMQueryHandler.ATHENA_OSM_OUTPUT_BUCKET);

	      // Create the StartQueryExecutionRequest to send to Athena which will start the query.
	      StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
	              .withQueryString(DiSARMOSMQueryHandler.ATHENA_OSM_QUERY_ALL_BUIILDINGS)
	              .withQueryExecutionContext(queryExecutionContext)
	              .withResultConfiguration(resultConfiguration);
			
	      StartQueryExecutionResult startQueryExecutionResult = client.startQueryExecution(startQueryExecutionRequest);
	      return startQueryExecutionResult.getQueryExecutionId();
	      
	  }
	
	
}
