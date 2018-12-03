/**
 * DiSARMAthenaClientFactory to build Athena with the following properties:
 * - Set the region of the client
 * - Use the instance profile from the EC2 instance as the credentials provider
 * - Configure the client to increase the execution timeout.
 * @author  Vikram Sridharan
 * @version 1.0
 * @since   2018-10-15 
 */

package com.ucsf.disarm.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;

import java.util.Properties;

public class DiSARMAthenaClientFactory 
{
	
  
  
  //configuring parameters for the connection
  int CLIENT_EXECUTION_TIME_OUT = 30*60000; //30 minutes
  
  public AmazonAthena createClient()
  {
	  BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAI3KSYT5ANVSFS3FQ", "YXGTV02EmjcdhAGDmxd73OlihNqg3nai29BGqr04");
	  AmazonAthenaClientBuilder builder = AmazonAthenaClientBuilder.standard()
	          .withRegion(Regions.US_EAST_1)
	          .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
	          .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(this.CLIENT_EXECUTION_TIME_OUT));
	  
      return builder.build();
  }	

}
