package com.github.pires.obd.reader.activity;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.mobileconnectors.kinesis.kinesisrecorder.KinesisRecorder;
import com.amazonaws.mobileconnectors.kinesis.kinesisrecorder.internal.FileRecordStore;
import com.amazonaws.mobileconnectors.kinesis.kinesisrecorder.internal.JSONRecordAdapter;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by andi on 21.12.15.
 */
public class OBDKinesisRecorder extends KinesisRecorder {

    public OBDKinesisRecorder(File directory, Regions region,AWSCredentialsProvider credentialsProvider) {
        super(directory, region, credentialsProvider);
    }
    /** TheRecordStore is responsible for saving requests to be sent later **/
    FileRecordStore recordStore;
    /**
     * The RecordAdapter is responsible for converting PutRecordRequests to and
     * from JSON
     **/
    private JSONRecordAdapter adapter;

    public void saveRecord(byte[] data, String streamName, String partitionKey) {
        if (streamName == null || streamName.isEmpty() || data == null || data.length < 1) {
            throw new IllegalArgumentException(
                    "You must pass a non-null, non-empty stream name and non-empty data");
        }
        PutRecordRequest putRequest = new PutRecordRequest().withData(ByteBuffer.wrap(data))
                .withStreamName(streamName)
                .withPartitionKey(partitionKey);
        try {
            recordStore.put(adapter.translateFromRecord(putRequest).toString());
        } catch (IOException e) {
            throw new AmazonClientException("Error saving record", e);
        }
    }
}
