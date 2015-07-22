package de.dengpeng.projects;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;;

/**
 * Processes records and checkpoints progress.
 */
public class RecordProcessor implements IRecordProcessor {
	private static final Log LOG = LogFactory.getLog(RecordProcessor.class);
	private String kinesisShardId;

	// Backoff and retry settings
	private static final int NUM_RETRIES = 10;
	private static final long BACKOFF_TIME_IN_MILLIS = 3000L;

	// Checkpoint about once a minute
	private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
	private long nextCheckpointTimeInMillis;
	
	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	public void initialize(InitializationInput initializationInput) {
		this.kinesisShardId = initializationInput.getShardId();
		
		LOG.info("Initializing record processor for shard: " + kinesisShardId);
	}

	public void processRecords(ProcessRecordsInput processRecordsInput) {
		LOG.info("Processing " + processRecordsInput.getRecords().size() + " records from " + kinesisShardId);
		
		// Process records and perform all exception handling.
		processRecordsWithRetries(processRecordsInput.getRecords());

		// Checkpoint once every checkpoint interval.
		if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
			checkpoint(processRecordsInput.getCheckpointer());
			nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
		}
	}

	public void shutdown(ShutdownInput shutdownInput) {
		LOG.info("Shutting down record processor for shard: " + kinesisShardId);
		// Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
		System.out.println(shutdownInput.getShutdownReason().name());
		
		if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
			checkpoint(shutdownInput.getCheckpointer());
		}
	}

	
    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     * 
     * @param records Data records to be processed.
     */
	private void processRecordsWithRetries(List<Record> records) {
		for (Record record : records) {
			boolean processedSuccessfully = false;
			for (int i = 0; i < NUM_RETRIES; i++) {
				try {
					//
					// Logic to process record goes here.
					//
					processSingleRecord(record);

					processedSuccessfully = true;
					break;
				} catch (Throwable t) {
					LOG.warn("Caught throwable while processing record " + record, t);
				}

				// backoff if we encounter an exception.
				try {
					Thread.sleep(BACKOFF_TIME_IN_MILLIS);
				} catch (InterruptedException e) {
					LOG.debug("Interrupted sleep", e);
				}
			}

			if (!processedSuccessfully) {
				LOG.error("Couldn't process record " + record + ". Skipping the record.");
			}
		}
	}
	
    private void processSingleRecord(Record record) {
    	// TODO Add your own record processing logic here
    	
        String data = null;
        try {
            // For this app, we interpret the payload as UTF-8 chars.
            data = decoder.decode(record.getData()).toString();
            // Assume this record came from AmazonKinesisSample and log its age.
            
            System.out.println(data);
            
        } catch (NumberFormatException e) {
        	LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
        } catch (CharacterCodingException e) {
        	LOG.error("Malformed data: " + data, e);
        }
    }
	
    /** Checkpoint with retries.
     * @param checkpointer
     */
	private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
		LOG.info("Checkpointing shard " + kinesisShardId);
		
		for (int i = 0; i < NUM_RETRIES; i++) {
			try {
				checkpointer.checkpoint();
				break;
			} catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
				break;
			} catch (ThrottlingException e) {
				// Backoff and re-attempt checkpoint upon transient failures
				if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
				}else{
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
				}
			} catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
			}
			try {
				Thread.sleep(BACKOFF_TIME_IN_MILLIS);
			} catch (InterruptedException e) {
				LOG.debug("Interrupted sleep", e);
			}
		}
	}

}
