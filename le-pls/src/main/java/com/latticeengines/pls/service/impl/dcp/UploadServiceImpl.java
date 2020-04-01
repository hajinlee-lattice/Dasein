package com.latticeengines.pls.service.impl.dcp;

import java.util.List;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
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
    public void downloadUpload(String uploadId, HttpServletRequest request, HttpServletResponse response) {
        String tenantId = MultiTenantContext.getShortTenantId();
        Upload upload = uploadProxy.getUpload(tenantId, Long.parseLong(uploadId));
        UploadConfig config = upload.getUploadConfig();
        String importPath = config.getUploadImportedFilePath();
        String errorPath = config.getUploadImportedErrorFilePath();


    }
}
