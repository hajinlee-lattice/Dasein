package com.latticeengines.cdl.workflow.steps.importdata;


import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.Lists;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.CreateDataTemplateRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.ImportListSegmentWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.CopyListSegmentCSVConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("copyListSegmentCSV")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CopyListSegmentCSV extends BaseWorkflowStep<CopyListSegmentCSVConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CopyListSegmentCSV.class);

    private static final long GB = 1024L * 1024L * 1024L;

    @Inject
    private S3Service s3Service;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private YarnConfiguration yarnConfiguration;

    @Value("${camille.zk.pod.id:Default}")
    protected String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    protected HdfsToS3PathBuilder pathBuilder;

    @Override
    public void execute() {
        pathBuilder = new HdfsToS3PathBuilder(useEmr);
        String tenantId = configuration.getCustomerSpace().getTenantId();
        String segmentName = configuration.getSegmentName();
        String sourceBucket = configuration.getSourceBucket();
        String sourceKey = configuration.getSourceKey();
        checkFileSize(sourceBucket, sourceKey);
        String destBucket = configuration.getDestBucket();
        CreateDataTemplateRequest request = new CreateDataTemplateRequest();
        request.setTemplateKey(ListSegment.RAW_IMPORT);
        String templateId = segmentProxy.createOrUpdateDataTemplate(tenantId, segmentName, request);
        String templateDir = pathBuilder.getS3AtlasDataTemplatePrefix(destBucket, tenantId, templateId);
        templateDir = templateDir.substring(templateDir.indexOf(destBucket) + destBucket.length() + 1);
        String fileName = FilenameUtils.getName(sourceKey);
        log.info("templateDir is {}, fileName is {}.", templateDir, fileName);
        S3DataUnit dataUnit = createImportUnit(destBucket, tenantId, templateId, templateDir, fileName);
        dataUnit = (S3DataUnit) dataUnitProxy.create(tenantId, dataUnit);
        putStringValueInContext(ImportListSegmentWorkflowConfiguration.IMPORT_DATA_UNIT_NAME, dataUnit.getName());
        copyToDataTemplateFolder(sourceBucket, sourceKey, destBucket, dataUnit.getPrefix());
        uncompressZipCSV(dataUnit);
    }

    private void uncompressZipCSV(S3DataUnit s3DataUnit) {
        try {
            InputStream in = s3Service.readObjectAsStream(s3DataUnit.getBucket(), s3DataUnit.getPrefix());
            String zipTempPath = s3DataUnit.getLinkedHdfsPath() + "/temp.zip";
            log.info("zipTempPath is " + zipTempPath);
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, in, zipTempPath);
            log.info("zipTempPath is " + zipTempPath + " transfer done");
            HdfsUtils.uncompressZipFileWithinHDFS(yarnConfiguration, zipTempPath, s3DataUnit.getLinkedHdfsPath());
        } catch (IOException exception) {
            log.error("uncompress zip file failed with data unit {}.", s3DataUnit.getName());
        }
    }

    private S3DataUnit createImportUnit(String bucket, String tenantId, String templateId, String prefix, String fileName) {
        S3DataUnit dataUnit = new S3DataUnit();
        dataUnit.setTenant(tenantId);
        dataUnit.setDataTemplateId(templateId);
        String dataUnitName = NamingUtils.uuid(ListSegment.RAW_IMPORT);
        dataUnit.setName(dataUnitName);
        dataUnit.setBucket(bucket);
        dataUnit.setPrefix(prefix + "/" + dataUnit.getName() + "/" + fileName);
        dataUnit.setDataFormat(DataUnit.DataFormat.ZIP);
        dataUnit.setRoles(Lists.newArrayList(DataUnit.Role.Import));
        dataUnit.setLinkedHdfsPath(pathBuilder.getHdfsAtlasDataUnitPrefix(podId, tenantId, templateId, dataUnitName));
        return dataUnit;
    }

    private void checkFileSize(String s3Bucket, String fileKey) {
        ObjectMetadata objectMetadata = s3Service.getObjectMetadata(s3Bucket, fileKey);
        if (objectMetadata == null) {
            throw new RuntimeException(String.format("Cannot get metadata for file %s : %s", s3Bucket, fileKey));
        }
        if (objectMetadata.getContentLength() > 100L * GB) {
            throw new LedpException(LedpCode.LEDP_40078);
        }
    }

    private void copyToDataTemplateFolder(String sourceBucket, String sourceKey, String destBucket, String destKey) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AmazonS3Exception.class), null);
        retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info(String.format("(Attempt=%d) Retry copying object from %s:%s to %s:%s", //
                        context.getRetryCount() + 1, sourceBucket, sourceKey, destBucket, destKey));
            }
            s3Service.copyObject(sourceBucket, sourceKey, destBucket, destKey);
            return true;
        });
    }
}
