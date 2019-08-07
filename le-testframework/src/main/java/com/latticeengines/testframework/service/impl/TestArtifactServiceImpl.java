package com.latticeengines.testframework.service.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

@Service("testArtifactService")
public class TestArtifactServiceImpl implements TestArtifactService {

    private static final Logger log = LoggerFactory.getLogger(TestArtifactServiceImpl.class);

    private static final String S3_BUCKET = "latticeengines-test-artifacts";

    private static final String DOWNLOAD_DIR = "s3downloads";
    private static final int BUFFER_SIZE = 1024 * 1024; // 1M
    private static final int DOWNLOAD_LOG_INTERVAL = 10 * 1024 * 1024; // 10M

    private AmazonS3 S3;

    @Inject
    public TestArtifactServiceImpl(@Qualifier(value = "awsCredentials") BasicAWSCredentials basicAWSCredentials) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSocketTimeout(120000);
        this.S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials)) //
                .withRegion("us-east-1") //
                .withClientConfiguration(clientConfiguration) //
                .enableAccelerateMode() //
                .build();
        String accelerateStatus = this.S3.getBucketAccelerateConfiguration(new GetBucketAccelerateConfigurationRequest(S3_BUCKET)).getStatus();
        log.info("Acceleration status of bucket " + S3_BUCKET + " is " + accelerateStatus);
    }

    @Override
    public InputStream readTestArtifactAsStream(String objectDir, String version, String fileName) {
        String objectKey = objectKey(objectDir, version, fileName);
        if (S3.doesObjectExist(S3_BUCKET, objectKey)) {
            GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET, objectKey);
            try {
                S3Object s3Object = S3.getObject(getObjectRequest);
                log.info(String.format("Reading the test artifact %s of type %s and size %s", objectKey,
                        s3Object.getObjectMetadata().getContentType(),
                        FileUtils.byteCountToDisplaySize(s3Object.getObjectMetadata().getContentLength())));
                return s3Object.getObjectContent();
            } catch (AmazonS3Exception e) {
                throw new RuntimeException("Failed to get object " + objectKey + " from S3 bucket " + S3_BUCKET, e);
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean testArtifactExists(String objectDir, String version, String fileName) {
        String objectKey = objectKey(objectDir, version, fileName);
        return S3.doesObjectExist(S3_BUCKET, objectKey);
    }

    @Override
    public boolean testArtifactFolderExists(String baseDir, String version, String folder) {
        String folderKey = folderPrefix(objectKey(baseDir, version, folder));
        return S3.listObjectsV2(S3_BUCKET, folderKey).getKeyCount() > 0;
    }

    @Override
    public void copyTestArtifactFolder(String baseDir, String version, String folder, String targetBucket,
            String targetPrefix) {
        String sourcePrefix = folderPrefix(objectKey(baseDir, version, folder));
        targetPrefix = folderPrefix(targetPrefix);
        ListObjectsV2Result objects = S3.listObjectsV2(S3_BUCKET, sourcePrefix);
        for (S3ObjectSummary object : objects.getObjectSummaries()) {
            String fileName = object.getKey().substring(object.getKey().lastIndexOf("/") + 1);
            String targetKey = targetPrefix + fileName;
            log.info("Copy from s3://{}/{} to s3://{}/{}", S3_BUCKET, object.getKey(), targetBucket, targetKey);
            S3.copyObject(S3_BUCKET, object.getKey(), targetBucket, targetKey);
        }
    }

    @Override
    public synchronized File downloadTestArtifact(String objectDir, String version, String fileName) {
        String objectKey = objectKey(objectDir, version, fileName);
        GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET, objectKey);

        S3Object s3Object;
        try {
            s3Object = S3.getObject(getObjectRequest);
        } catch (AmazonS3Exception e) {
            throw new RuntimeException("Failed to download object " + objectKey + " from S3 bucket " + S3_BUCKET, e);
        }

        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
        String eTag = objectMetadata.getETag();
        File dir = new File(DOWNLOAD_DIR + File.separator + eTag);
        String outputFileName = dir.getPath() + File.separator + fileName;
        File outputFile = new File(outputFileName);
        String downloadingFlag = dir.getPath() + File.separator + "_DOWNLOADING_";
        File downloadingFlagFile = new File(downloadingFlag);

        while (downloadingFlagFile.exists()) {
            log.info("Another process is downloading the file ...");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (outputFile.exists() && FileUtils.sizeOf(outputFile) >= objectMetadata.getContentLength()) {
            // same ETag already downloaded
            log.info("Target file " + outputFileName + " already exists, skip downloading.");
        } else {
            // create target dir and download the file
            log.info(String.format("Attempt to download the test artifact %s of type %s and size %s to %s", objectKey,
                    objectMetadata.getContentType(),
                    FileUtils.byteCountToDisplaySize(objectMetadata.getContentLength()), outputFile.getPath()));
            createTargetDir(dir);
            try {
                FileUtils.touch(downloadingFlagFile);
                downloadS3Object(s3Object, outputFile);
            } catch (IOException e) {
                throw new RuntimeException("Failed to download test artifact from S3.", e);
            } finally {
                FileUtils.deleteQuietly(downloadingFlagFile);
            }
        }
        return outputFile;
    }

    private void downloadS3Object(S3Object s3Object, File outputFile) {
        InputStream is = s3Object.getObjectContent();
        byte[] content = new byte[BUFFER_SIZE];
        double totalSize = Long.valueOf(s3Object.getObjectMetadata().getContentLength()).doubleValue();
        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            int totalRead = 0;
            int bytesRead;
            double progress;
            int lastLoggedBytes = 0;
            log.info(String.format("%s: start downloading from S3", outputFile.getName()));
            while ((bytesRead = is.read(content)) != -1) {
                outputStream.write(content, 0, bytesRead);
                totalRead += bytesRead;
                progress = 100.0 * totalRead / totalSize;
                if (totalRead - lastLoggedBytes >= DOWNLOAD_LOG_INTERVAL) {
                    log.info(String.format("%s: %s ( %.2f %% ) downloaded from S3", //
                            outputFile.getName(), FileUtils.byteCountToDisplaySize(totalRead), progress));
                    lastLoggedBytes = totalRead;
                }
            }
            log.info(String.format("%s: %s downloaded from S3", outputFile.getName(),
                    FileUtils.byteCountToDisplaySize(totalRead)));
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to stream the content of S3 object %s to local file %s",
                    s3Object.getKey(), outputFile.getPath()), e);
        }
    }

    private String createTargetDir(File dir) {
        if (dir.exists()) {
            FileUtils.deleteQuietly(dir);
        }
        try {
            FileUtils.forceMkdir(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create download directory " + dir.getPath(), e);
        }
        return dir.getPath();
    }

    private String objectKey(String objectDir, String version, String fileName) {
        return String.format("%s/%s/%s", objectDir, version, fileName).replace("//", "/");
    }

    private String folderPrefix(String prefix) {
        if (prefix.endsWith("/")) {
            return prefix;
        }
        return prefix + "/";
    }

}
