package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.latticeengines.apps.dcp.entitymgr.UploadEntityMgr;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;

@Service("uploadService")
public class UploadServiceImpl implements UploadService {

    @Inject
    private UploadEntityMgr uploadEntityMgr;

    @Override
    public List<Upload> getUploads(String customerSpace, String sourceId) {
        return uploadEntityMgr.findBySourceId(sourceId);
    }

    @Override
    public List<Upload> getUploads(String customerSpace, String sourceId, Upload.Status status) {
        return uploadEntityMgr.findBySourceIdAndStatus(sourceId, status);
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
    public void updateUploadConfig(String customerSpace, Long uploadPid, UploadConfig uploadConfig) {
        Upload upload = uploadEntityMgr.findByPid(uploadPid);
        if (upload == null) {
            throw new RuntimeException("Cannot find Upload record with Pid: " + uploadPid);
        }
        upload.setUploadConfig(uploadConfig);
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
