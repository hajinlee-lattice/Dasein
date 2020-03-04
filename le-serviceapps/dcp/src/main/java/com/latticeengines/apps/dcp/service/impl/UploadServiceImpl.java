package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.UploadEntityMgr;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.domain.exposed.dcp.Upload;

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
}
