package asperacos.asperacos;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.SDKGlobalConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.aspera.transfer.AsperaConfig;
import com.ibm.cloud.objectstorage.services.aspera.transfer.AsperaTransaction;
import com.ibm.cloud.objectstorage.services.aspera.transfer.AsperaTransferManager;
import com.ibm.cloud.objectstorage.services.aspera.transfer.AsperaTransferManagerBuilder;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;


public class FileUploader 
{
	
	private static String COS_ENDPOINT = "https://s3.us-south.cloud-object-storage.appdomain.cloud"; // eg "https://s3.us.cloud-object-storage.appdomain.cloud"
	private static String COS_API_KEY_ID = "G3ArjF0WS8gEcRauUe5E9PU9BFvrI5zBxQsQgsMimOy_"; // eg "0viPHOY7LbLNa9eLftrtHPpTjoGv6hbLD1QalRXikliJ"
	private static String COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token";
	private static String COS_SERVICE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/cfa3677a93e716f7caaab653514dc6b3:808f2b3d-b726-4f26-8179-83a0373443b4::"; // "crn:v1:bluemix:public:iam-identity::a/3ag0e9402tyfd5d29761c3e97696b71n::serviceid:ServiceId-540a4a41-7322-4fdd-a9e7-e0cb7ab760f9"
	private static String COS_BUCKET_LOCATION = "us-south"; // eg "us"
	
	 private static AmazonS3 cosClient;
	
    public static void main( String[] args )
    {
    	System.out.println("STARTING");
    	SDKGlobalConfiguration.IAM_ENDPOINT = COS_AUTH_ENDPOINT;
    	
        try
        {
        	cosClient=createClient(COS_API_KEY_ID, COS_SERVICE_CRN, COS_ENDPOINT, COS_BUCKET_LOCATION);
        
        System.out.println("CLIENT CREATED");
        
    	uploadAspera("fxnbkt","/home/fnaranjo/main.cf");
    	
        }catch(Exception ex){
        	System.out.println(ex.toString());
        }
    }
    
    private static AmazonS3 createClient(String api_key, String service_instance_id, String endpoint_url, String location)
    {
        AWSCredentials credentials = new BasicIBMOAuthCredentials(api_key, service_instance_id);
        ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(5000);
        clientConfig.setUseTcpKeepAlive(true);

        AmazonS3 cos = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new EndpointConfiguration(endpoint_url, location)).withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfig).build();

        return cos;
    }
    
    private static void uploadAspera(String bucketName,String filePath) throws InterruptedException, ExecutionException
    {
    	System.out.println("Staring upload using Aspera");
    	File inputFile = new File(filePath);
    	AsperaConfig asperaConfig = new AsperaConfig()
    	    .withMultiSession(2)
    	    .withMultiSessionThresholdMb(60);

    	AsperaTransferManager asperaTransferMgr = new AsperaTransferManagerBuilder(COS_API_KEY_ID,cosClient).withAsperaConfig(asperaConfig).build();
    	
    	Future<AsperaTransaction> asperaTransactionFuture = asperaTransferMgr.upload(bucketName, inputFile, inputFile.getName());
    	AsperaTransaction asperaTransaction = asperaTransactionFuture.get();
    
    }
}
