package com.latticeengines.testframework.exposed.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

public class S3Utilities {

    protected S3Utilities() {
        throw new UnsupportedOperationException();
    }

    private static AmazonS3 s3Client;
    private static Logger logger = LoggerFactory.getLogger(S3Utilities.class);

    private static final String CLIENT_REGION = "us-east-1";

    static {
        s3Client = AmazonS3ClientBuilder.standard().withRegion(CLIENT_REGION)
                .withCredentials(new ProfileCredentialsProvider())
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance()).build();
    }

    public static void setS3ClientWithCredentials(String accessKey, String secretKey) {
        logger.info("Connect to S3 with {}, {}", accessKey, secretKey);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        s3Client = AmazonS3ClientBuilder.standard().withRegion(CLIENT_REGION)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
    }

    public static void download(String bucketName, String key, String desFilePath)
            throws AmazonServiceException, IOException {
        logger.info("Downloading {}://{} to {}", bucketName, key, desFilePath);
        S3Object o = s3Client.getObject(bucketName, key);
        try (S3ObjectInputStream s3is = o.getObjectContent();
                FileOutputStream fos = new FileOutputStream(new File(desFilePath))) {
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
        } catch (AmazonServiceException e) {
            logger.error(e.getErrorMessage());
            throw e;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static ObjectMetadata getObjectSummary(String bucketName, String key) throws AmazonServiceException {
        logger.info("GetObjectSummary {}://{}", bucketName, key);
        S3Object o = s3Client.getObject(bucketName, key);
        try {
            ObjectMetadata objectMetadata = o.getObjectMetadata();
            return objectMetadata;
        } catch (AmazonServiceException e) {
            logger.error(e.getErrorMessage());
            throw e;
        }
    }

    public static void downloadAll(String bucketName, String keyFolder, String desDirectory)
            throws AmazonServiceException, IOException {
        File des = new File(desDirectory);
        if (!des.exists()) {
            des.mkdirs();
        }
        List<String> keyList = getObjectListFromFolder(bucketName, keyFolder);
        for (String key : keyList) {
            download(bucketName, key, Paths.get(desDirectory, FilenameUtils.getName(key)).toString());
        }
    }

    public static boolean downloadDirectory(String bucketName, String keyFolder, String desDirectory) {
        TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        File des = new File(desDirectory);
        if (!des.exists()) {
            des.mkdirs();
        }
        MultipleFileDownload download = transferManager.downloadDirectory(bucketName, keyFolder, des);
        try {
            download.waitForCompletion();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            return false;
        }
        transferManager.shutdownNow(false);
        return true;
    }

    public static boolean keyExists(String bucketName, String key) {
        ObjectListing objectListing = s3Client.listObjects(bucketName);
        for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
            if (os.getKey().startsWith(key)) {
                return true;
            }
        }
        return false;
    }

    public static void copyObject(String fromBucket, String fromObjectKey, String toBucket, String toObjectKey)
            throws AmazonServiceException {
        try {
            logger.info("Copying from {}://{} to {}://{}", fromBucket, fromObjectKey, toBucket, toObjectKey);
            s3Client.copyObject(fromBucket, fromObjectKey, toBucket, toObjectKey);
        } catch (AmazonServiceException e) {
            logger.error(e.getErrorMessage());
            throw e;
        }
    }

    public static List<String> getObjectListFromFolder(String bucketName, String folderKey) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName)
                .withPrefix(folderKey + "/");

        List<String> keys = new ArrayList<>();
        ObjectListing objects = s3Client.listObjects(listObjectsRequest);
        for (;;) {
            List<S3ObjectSummary> summaries = objects.getObjectSummaries();
            if (summaries.size() < 1) {
                break;
            }
            summaries.forEach(s -> keys.add(s.getKey()));
            objects = s3Client.listNextBatchOfObjects(objects);
        }

        return keys;
    }

    public static List<String> getObjectListFromFolder(String bucketName, String folderKey, String fileExtensionName) {
        List<String> allObjectKeys = getObjectListFromFolder(bucketName, folderKey);
        logger.info("Got object keys: {}", Arrays.toString(allObjectKeys.toArray()));
        return allObjectKeys.stream().filter(i -> FilenameUtils.getExtension(i).equals(fileExtensionName)).sorted()
                .collect(Collectors.toList());
    }

    public static List<String> getFileNameListFromFolder(String bucketName, String folderKey,
            String fileExtensionName) {
        logger.info("Get bucket {}, key folder {} with file extension {}", bucketName, folderKey, fileExtensionName);
        return getObjectListFromFolder(bucketName, folderKey, fileExtensionName).stream()
                .map(i -> FilenameUtils.getName(i)).collect(Collectors.toList());
    }

    public static void uploadFileToS3(String bucketName, File file) throws AmazonServiceException {
        try {
            logger.debug("Copying file {} , to S3 bucket: {}", file.getName(), bucketName);
            s3Client.putObject(new PutObjectRequest(bucketName, file.getName(), file));
            logger.info("The file is successfully stored in {} ", bucketName + "/" + file.getName());
        } catch (AmazonServiceException e) {
            logger.error(e.getErrorMessage());
            throw e;
        }
    }

    /**
     * return the file name of lastest export file
     *
     */
    public static String getLatestUpldatedFile(String bucket, String fileName) {
        LinkedHashMap<Integer, Date> datetimeSort = new LinkedHashMap<>();
        String lastModifiedFileName = null;
        ObjectListing objects = s3Client.listObjects(bucket, fileName);
        do {
            for (int i = 0; i < objects.getObjectSummaries().size(); i++) {
                datetimeSort.put(i, objects.getObjectSummaries().get(i).getLastModified());
            }
            try {
                lastModifiedFileName = objects.getObjectSummaries().get(getLatestDateFile(datetimeSort)).getKey()
                        .toString();
                logger.info("Download file name as: " + lastModifiedFileName);
            } catch (Exception ex) {
                throw ex;
            }
            objects = s3Client.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());

        return lastModifiedFileName;

    }

    /**
     * calculate last modified date name
     * 
     * @param map
     * @return
     */
    private static int getLatestDateFile(LinkedHashMap<Integer, Date> map) {
        if (map == null)
            return -1;
        Collection<Date> collection = map.values();
        Object[] obj = collection.toArray();
        Arrays.sort(obj);
        for (Object key : map.keySet()) {
            if (map.get(key).equals(obj[obj.length - 1])) {
                return (int) key;
            }
        }
        return -1;
    }

}
