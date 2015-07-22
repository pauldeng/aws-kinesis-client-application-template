package de.dengpeng.projects;
import java.net.InetAddress;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * Sample Amazon Kinesis Application.
 */
public class Main {
	
    /*
     * Before running the code:
     *    If you are running this locally, please:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (~/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *      
     *    If you are running this on EC2 or Elastic Beanstalk, please:
     *      Specify IAM role with access to Kinesis and CloudWatch 
     *      
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

	private static final String SAMPLE_APPLICATION_NAME = "pdeng_test_stream_app";
	
	private static final String SAMPLE_APPLICATION_STREAM_NAME = "test_stream";
	
	private static final String AWS_REGION_NAME = "us-west-2";
	
    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM = InitialPositionInStream.LATEST;
	
	private static AWSCredentialsProvider credentialsProvider;

	
	public static void main(String[] args) throws Exception {
		
		init();
		
		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		
		KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
				SAMPLE_APPLICATION_STREAM_NAME, credentialsProvider, workerId);
		
		config.withRegionName(AWS_REGION_NAME);
		config.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);
		
		IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();
		Worker worker = new Worker.Builder()
				.recordProcessorFactory(recordProcessorFactory)
				.config(config)
				.build();
		
		 System.out.printf("Running %s to process stream %s as worker %s...\n",
	                SAMPLE_APPLICATION_NAME,
	                SAMPLE_APPLICATION_STREAM_NAME,
	                workerId);
		 
	        int exitCode = 0;
	        try {
	            worker.run();
	        } catch (Throwable t) {
	            System.err.println("Caught throwable while processing data.");
	            t.printStackTrace();
	            exitCode = 1;
	        }
	        System.exit(exitCode);
	}

	private static void init() {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");
		
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
		credentialsProvider = new DefaultAWSCredentialsProviderChain();
		try {
			credentialsProvider.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
	}

	public static void run() throws Exception {
		init();
		

		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

		KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
				SAMPLE_APPLICATION_STREAM_NAME, credentialsProvider, workerId);
		config.withRegionName(AWS_REGION_NAME);
		config.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);
		
		IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();
		Worker worker = new Worker.Builder()
				.recordProcessorFactory(recordProcessorFactory)
				.config(config)
				.build();
		
		 System.out.printf("Running %s to process stream %s as worker %s...\n",
	                SAMPLE_APPLICATION_NAME,
	                SAMPLE_APPLICATION_STREAM_NAME,
	                workerId);
		 
	        int exitCode = 0;
	        try {
	            worker.run();
	        } catch (Throwable t) {
	            System.err.println("Caught throwable while processing data.");
	            t.printStackTrace();
	            exitCode = 1;
	        }
	        System.exit(exitCode);
	}
}
