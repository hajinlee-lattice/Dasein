package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.S3ExportFolderService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

@Component("s3ExportFolderService")
public class S3ExportFolderServiceImpl implements S3ExportFolderService {

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    private HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();

    @Override
    public String getDropFolderExportPath(String customerSpace, AtlasExportType exportType, String datePrefix, String optionalId) {
        return dropBoxService.getExportPath(customerSpace, exportType, datePrefix, optionalId);
    }

    @Override
    public String getSystemExportPath(String customerSpace) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        String systemExportPath = pathBuilder.getS3AtlasExportFileDir(s3Bucket, tenantId);
        String strippedPath = pathBuilder.stripProtocolAndBucket(systemExportPath);
        if (!strippedPath.endsWith("/")) {
            strippedPath += "/";
        }
        if (!s3Service.objectExist(s3Bucket, strippedPath)) {
            s3Service.createFolder(s3Bucket, strippedPath);
        }
        return strippedPath;
    }

    @Override
    public String getS3PathWithProtocol(String customerSpace, String relativePath) {
        if (!relativePath.startsWith("/")) {
            relativePath = "/" + relativePath;
        }
        return pathBuilder.getS3BucketDir(s3Bucket) + relativePath;
    }

}
