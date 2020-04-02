package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.latticeengines.apps.dcp.entitymgr.UploadEntityMgr;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.metadata.service.MetadataService;

@Service("uploadService")
public class UploadServiceImpl implements UploadService {

    private static final Logger log = LoggerFactory.getLogger(UploadServiceImpl.class);

    @Inject
    private UploadEntityMgr uploadEntityMgr;

    @Inject
    private MetadataService metadataService;

    @Override
    public List<Upload> getUploads(String customerSpace, String sourceId) {
        return uploadEntityMgr.findBySourceId(sourceId);
    }

    @Override
    public List<Upload> getUploads(String customerSpace, String sourceId, Upload.Status status) {
        return uploadEntityMgr.findBySourceIdAndStatus(sourceId, status);
    }

    @Override
    public Upload getUpload(String customerSpace, Long pid) {
        log.info("Try find upload in " + customerSpace + " with pid " + pid);
        return uploadEntityMgr.findByPid(pid);
    }

    @Override
    public Upload createUpload(String customerSpace, String sourceId, UploadConfig uploadConfig) {
        if (StringUtils.isEmpty(sourceId)) {
            throw new RuntimeException("Cannot create upload bind with empty sourceId!");
        }
        Upload upload = new Upload();
        upload.setSourceId(sourceId);
        upload.setTenant(MultiTenantContext.getTenant());
        upload.setStatus(Upload.Status.NEW);
        upload.setUploadConfig(uploadConfig);
        uploadEntityMgr.create(upload);

        return upload;
    }

    @Override
    public void registerMatchResult(String customerSpace, long uploadPid, String tableName) {
        Upload upload = uploadEntityMgr.findByPid(uploadPid);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with Pid: " + uploadPid);
        }
        Table table = metadataService.getTable(CustomerSpace.parse(customerSpace), tableName);
        if (table == null) {
            throw new RuntimeException("Cannot find Upload match result table with name: " + tableName);
        }
        String oldTableName = upload.getMatchResultName();
        Table oldTable = metadataService.getTable(CustomerSpace.parse(customerSpace), oldTableName);
        boolean hasOldTable = oldTable != null;
        upload.setMatchResult(table);
        uploadEntityMgr.update(upload);
        if (hasOldTable) {
            log.info("There was an old match result table {}, going to be marked as temporary.", oldTableName);
            RetentionPolicy retentionPolicy = RetentionPolicyUtil.toRetentionPolicy(7, RetentionPolicyTimeUnit.DAY);
            metadataService.updateTableRetentionPolicy(CustomerSpace.parse(customerSpace), oldTableName, retentionPolicy);
        } else {
            log.info("There was no old match result table.");
        }
    }

    @Override
    public void updateUploadConfig(String customerSpace, Long uploadPid, UploadConfig uploadConfig) {
        Upload upload = uploadEntityMgr.findByPid(uploadPid);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with Pid: " + uploadPid);
        }
        upload.setUploadConfig(uploadConfig);
        uploadEntityMgr.update(upload);
    }

    @Override
    public void updateUploadStats(String customerSpace, Long uploadPid, UploadStats uploadStats) {
        Upload upload = uploadEntityMgr.findByPid(uploadPid);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with Pid: " + uploadPid);
        }
        upload.setUploadStats(uploadStats);
        uploadEntityMgr.update(upload);
    }

    @Override
    public void updateUploadStatus(String customerSpace, Long uploadPid, Upload.Status status) {
        Upload upload = uploadEntityMgr.findByPid(uploadPid);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with Pid: " + uploadPid);
        }
        upload.setStatus(status);
        uploadEntityMgr.update(upload);
    }
}
