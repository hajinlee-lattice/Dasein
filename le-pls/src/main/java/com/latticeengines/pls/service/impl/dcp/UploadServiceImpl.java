package com.latticeengines.pls.service.impl.dcp;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.pls.service.dcp.UploadService;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;


@Service
public class UploadServiceImpl implements UploadService {

    @Inject
    private UploadProxy uploadProxy;

    @Override
    public List<Upload> getAllBySourceId(String sourceId, Upload.Status status) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return uploadProxy.getUploads(customerSpace, sourceId, status);
    }

    @Override
    public Upload getByUploadId(long uploadPid) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return uploadProxy.getUpload(customerSpace, uploadPid);
    }

    @Override
    public void downloadUpload(String uploadId) {

    }
}
