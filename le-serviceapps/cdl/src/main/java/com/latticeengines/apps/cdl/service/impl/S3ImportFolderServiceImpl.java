package com.latticeengines.apps.cdl.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.S3ImportFolderService;
import com.latticeengines.aws.s3.S3Service;

@Component("s3ImportFolderService")
public class S3ImportFolderServiceImpl implements S3ImportFolderService {

    private static final Logger log = LoggerFactory.getLogger(S3ImportFolderServiceImpl.class);

    private static final String INPUT_ROOT = "/atlas/rawinput";
    private static final String RETRY = "/retry";
    private static final String IN_PROGRESS = "/inprogress";
    private static final String BACKUP = "/backup";
    private static final String COMPLETED = "/completed";
    private static final String SUCCEEDED = "/succeeded";
    private static final String FAILED = "/failed";

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;


    @Override
    public void initialize(String tenantId) {
        if (!s3Service.objectExist(s3Bucket, tenantId)) {
            s3Service.createFolder(s3Bucket, tenantId);
        }
        if (!s3Service.objectExist(s3Bucket, tenantId + INPUT_ROOT + "/")) {
            s3Service.createFolder(s3Bucket, tenantId + INPUT_ROOT + "/");
        }
        if (!s3Service.objectExist(s3Bucket, tenantId + INPUT_ROOT + BACKUP + "/")) {
            s3Service.createFolder(s3Bucket, tenantId + INPUT_ROOT + BACKUP + "/");
        }
        if (!s3Service.objectExist(s3Bucket, tenantId + INPUT_ROOT + IN_PROGRESS + "/")) {
            s3Service.createFolder(s3Bucket, tenantId + INPUT_ROOT + IN_PROGRESS + "/");
        }
        if (!s3Service.objectExist(s3Bucket, tenantId + INPUT_ROOT + RETRY + "/")) {
            s3Service.createFolder(s3Bucket, tenantId + INPUT_ROOT + RETRY + "/");
        }
        if (!s3Service.objectExist(s3Bucket, tenantId + INPUT_ROOT + COMPLETED + "/")) {
            s3Service.createFolder(s3Bucket, tenantId + INPUT_ROOT + COMPLETED + "/");
        }
        if (!s3Service.objectExist(s3Bucket, tenantId + INPUT_ROOT + COMPLETED + SUCCEEDED + "/")) {
            s3Service.createFolder(s3Bucket, tenantId + INPUT_ROOT + COMPLETED + SUCCEEDED + "/");
        }
        if (!s3Service.objectExist(s3Bucket, tenantId + INPUT_ROOT + COMPLETED + FAILED + "/")) {
            s3Service.createFolder(s3Bucket, tenantId + INPUT_ROOT + COMPLETED + FAILED + "/");
        }
    }

    @Override
    public String getBucket() {
        return s3Bucket;
    }

    @Override
    public Pair<String, String> startImport(String tenantId, String entity, String sourceBucket, String sourceKey) {
        initialize(tenantId);
        String date = dateFormat.format(new Date());
        String prefix = System.currentTimeMillis() / 1000L + "-" + entity;
        String path = tenantId + INPUT_ROOT + IN_PROGRESS + "/" + date + "/" + prefix + "/";
        String backupPath = tenantId + INPUT_ROOT + BACKUP + "/" + date + "/" + prefix + "/";
        if (!s3Service.objectExist(s3Bucket, path)) {
            s3Service.createFolder(s3Bucket, path);
        }
        if (!s3Service.objectExist(s3Bucket, backupPath)) {
            s3Service.createFolder(s3Bucket, backupPath);
        }
        String target = path + getFileName(sourceKey);
        String backupTarget = backupPath + getFileName(sourceKey);
        return Pair.of(target, backupTarget);
    }

    private String getFileName(String key) {
        if (StringUtils.isEmpty(key) || key.lastIndexOf('/') < 0) {
            return key;
        }
        return key.substring(key.lastIndexOf('/') + 1);
    }

    private String[] getParts(String key) {
        while (key.startsWith("/")) {
            key = key.substring(1);
        }
        String[] parts = key.split("/");
        return parts;
    }

    @Override
    public String moveFromInProgressToSucceed(String key) {
        if (!key.contains(IN_PROGRESS)) {
            return StringUtils.EMPTY;
        }
        String[] parts = getParts(key);
        String target = parts[0] + INPUT_ROOT + COMPLETED + SUCCEEDED + "/" + parts[4] + "/" + parts[5] + "/" +
                getFileName(key);
        s3Service.moveObject(s3Bucket, key, s3Bucket, target);
        return target;
    }

    @Override
    public String moveFromInProgressToFailed(String key) {
        if (!key.contains(IN_PROGRESS)) {
            return StringUtils.EMPTY;
        }
        String[] parts = getParts(key);
        String target = parts[0] + INPUT_ROOT + COMPLETED + FAILED + "/" + parts[4] + "/" + parts[5] + "/"  +
                getFileName(key);
        s3Service.moveObject(s3Bucket, key, s3Bucket, target);
        return target;
    }
}
