package com.github.pires.obd.reader.activity;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.mobileconnectors.kinesis.kinesisrecorder.KinesisRecorder;
import com.amazonaws.mobileconnectors.kinesis.kinesisrecorder.KinesisRecorderConfig;
import com.amazonaws.mobileconnectors.kinesis.kinesisrecorder.internal.FileRecordStore;
import com.amazonaws.mobileconnectors.kinesis.kinesisrecorder.internal.JSONRecordAdapter;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Inherited;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by andi on 21.12.15.
 */
public class OBDKinesisRecorder extends KinesisRecorder {

    FileRecordStore recordStore;
    AmazonKinesisClient client;
    private KinesisRecorderConfig config;
    private File directory;
    private JSONRecordAdapter adapter;

    public OBDKinesisRecorder(File directory, Regions region,AWSCredentialsProvider credentialsProvider) {
        this(directory, region, credentialsProvider, new KinesisRecorderConfig());
    }

    public OBDKinesisRecorder(File directory, Regions region,
                           AWSCredentialsProvider credentialsProvider, KinesisRecorderConfig config) {
        super(directory,region,credentialsProvider,config);
        if (directory == null || credentialsProvider == null || region == null || config == null) {
            throw new IllegalArgumentException(
                    "You must pass a non-null credentialsProvider, region, directory, and config to KinesisRecordStore");
        }

        this.directory = directory;
        this.config = new KinesisRecorderConfig(config);
        this.adapter = new JSONRecordAdapter();
        this.client = new AmazonKinesisClient(credentialsProvider,
                this.config.getClientConfiguration());
        client.setRegion(Region.getRegion(region));

        try {
            this.recordStore = new FileRecordStore(directory, this.config);
        } catch (IOException e) {
            throw new AmazonClientException("Unable to initialize KinesisRecorder", e);
        }

    }

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
