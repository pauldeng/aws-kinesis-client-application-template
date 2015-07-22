package de.dengpeng.projects;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * Used to create new record processors.
 */
public class RecordProcessorFactory implements IRecordProcessorFactory{
    
    public IRecordProcessor createProcessor() {
        return new RecordProcessor();
    }
}
