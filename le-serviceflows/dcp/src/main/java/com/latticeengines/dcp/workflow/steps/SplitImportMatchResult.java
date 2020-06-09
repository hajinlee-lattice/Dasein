package com.latticeengines.dcp.workflow.steps;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.dcp.SplitImportMatchResultJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SplitImportMatchResult extends RunSparkJob<ImportSourceStepConfiguration, SplitImportMatchResultConfig> {

    private static final Logger log = LoggerFactory.getLogger(SplitImportMatchResult.class);

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private SourceProxy sourceProxy;

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Override
    protected Class<SplitImportMatchResultJob> getJobClz() {
        return SplitImportMatchResultJob.class;
    }

    @Override
    protected SplitImportMatchResultConfig configureJob(ImportSourceStepConfiguration stepConfiguration) {
        String matchResultName = getStringValueFromContext(MATCH_RESULT_TABLE_NAME);
        Table matchResult = metadataProxy.getTable(configuration.getCustomerSpace().toString(), matchResultName);
        HdfsDataUnit input = matchResult.toHdfsDataUnit("input");
        SplitImportMatchResultConfig jobConfig = new SplitImportMatchResultConfig();
        jobConfig.setInput(Collections.singletonList(input));

        jobConfig.setMatchedDunsAttr(DataCloudConstants.ATTR_LDC_DUNS);

        List<ColumnMetadata> cms = matchResult.getColumnMetadata();
        log.info("InputSchema=" + JsonUtils.serialize(cms));
        List<ColumnMetadata> rejectedCms = cms.stream().filter(cm -> {
            boolean isInternal = (cm.getTagList() == null) || !cm.getTagList().contains(Tag.EXTERNAL);
            boolean isIdToExclude = isAttrToExclude(cm);
            return isInternal && !isIdToExclude;
        }).collect(Collectors.toList());
        Map<String, String> rejectedAttrs = convertToDispMap(rejectedCms);
        jobConfig.setRejectedAttrsMap(rejectedAttrs);

        List<ColumnMetadata> acceptedCms = cms.stream() //
                .filter(cm -> !isAttrToExclude(cm)).collect(Collectors.toList());
        Map<String, String> acceptedAttrs = convertToDispMap(acceptedCms);
        jobConfig.setAcceptedAttrsMap(acceptedAttrs);

        log.info("JobConfig=" + JsonUtils.serialize(jobConfig));
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String uploadId = configuration.getUploadId();
        UploadDetails upload = uploadProxy.getUploadByUploadId(customerSpace.toString(), uploadId);
        Source source = sourceProxy.getSource(customerSpace.toString(), configuration.getSourceId());
        ProjectDetails projectDetails = projectProxy.getDCPProjectByProjectId(customerSpace.toString(),
                configuration.getProjectId());
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace.toString());

        // create match result folder
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String uploadTS = upload.getUploadConfig().getUploadTSPrefix();
        String uploadMatchResultDir = UploadS3PathBuilderUtils.getUploadMatchResultDir(projectDetails.getProjectId(),
                source.getSourceId(), uploadTS);
        upload.getUploadConfig().setUploadMatchResultPrefix(uploadMatchResultDir);
        String matchResultPath = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, uploadMatchResultDir);
        if (!s3Service.objectExist(dropBoxSummary.getBucket(), matchResultPath)) {
            s3Service.createFolder(dropBoxSummary.getBucket(), matchResultPath);
            uploadProxy.updateUploadConfig(customerSpace.toString(), uploadId, upload.getUploadConfig());
        }

        // Copy files from spark workspace to upload result location.
        String acceptedCsvFilePath = getCsvFilePath(result.getTargets().get(0));
        String acceptedS3Path = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                upload.getUploadConfig().getUploadMatchResultAccepted());
        try {
            if (StringUtils.isNotEmpty(acceptedCsvFilePath)) {
                copyToS3(acceptedCsvFilePath, acceptedS3Path);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String rejectedCsvFilePath = getCsvFilePath(result.getTargets().get(1));
        String rejectedS3Path = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                upload.getUploadConfig().getUploadMatchResultRejected());
        try {
            if (StringUtils.isNotEmpty(rejectedCsvFilePath)) {
                copyToS3(rejectedCsvFilePath, rejectedS3Path);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        uploadProxy.updateUploadStatus(customerSpace.toString(), uploadId, Upload.Status.MATCH_FINISHED, null);

        updateUploadStatistics(result);
    }

    private void updateUploadStatistics(SparkJobResult result) {
        UploadStats stats = getObjectFromContext(UPLOAD_STATS, UploadStats.class);
        UploadStats.MatchStats matchStats = new UploadStats.MatchStats();
        matchStats.setAcceptedCnt(result.getTargets().get(0).getCount());
        matchStats.setPendingReviewCnt(0L);
        matchStats.setRejectedCnt(result.getTargets().get(1).getCount());
        stats.setMatchStats(matchStats);
        putObjectInContext(UPLOAD_STATS, stats);
    }

    private String getCsvFilePath(HdfsDataUnit dataUnit) {
        if (dataUnit.getCount() == 0) {
            return null;
        } else {
            return getFirstCsvFilePath(dataUnit);
        }
    }

    private Map<String, String> convertToDispMap(Collection<ColumnMetadata> cms) {
        return cms.stream().collect(Collectors.toMap(ColumnMetadata::getAttrName, cm -> {
            if (cm.getTagList() != null && cm.getTagList().contains(Tag.EXTERNAL)) {
                return "D&B " + cm.getDisplayName();
            } else {
                return cm.getDisplayName();
            }
        }));
    }

    private boolean isAttrToExclude(ColumnMetadata cm) {
        return  Arrays.asList(
                InterfaceName.InternalId.name(),
                InterfaceName.CustomerAccountId.name(),
                InterfaceName.LatticeAccountId.name()
        ).contains(cm.getAttrName());
    }

}
