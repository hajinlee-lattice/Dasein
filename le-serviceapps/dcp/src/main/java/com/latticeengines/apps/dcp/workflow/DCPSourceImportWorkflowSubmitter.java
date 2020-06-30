package com.latticeengines.apps.dcp.workflow;

import static com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion.OutOfBusiness;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchConfig;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;

@Component
public class DCPSourceImportWorkflowSubmitter extends WorkflowSubmitter {

    private static final String DEFAULT_DCP_S3_USER = "Default_DCP_S3_User";

    @Inject
    private UploadService uploadService;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private MatchRuleService matchRuleService;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, DCPImportRequest importRequest,
                                WorkflowPidWrapper pidWrapper) {
        UploadConfig uploadConfig = generateUploadConfig(customerSpace, importRequest);
        UploadDetails upload = uploadService.createUpload(customerSpace.toString(), importRequest.getSourceId(), uploadConfig);
        UploadStatsContainer container = new UploadStatsContainer();
        container = uploadService.appendStatistics(upload.getUploadId(), container);
        DCPSourceImportWorkflowConfiguration configuration =
                generateConfiguration(customerSpace, importRequest.getProjectId(), importRequest.getSourceId(), upload.getUploadId(),
                        container.getPid());
        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());
        Job job = workflowJobService.findByApplicationId(applicationId.toString());
        uploadService.updateStatsWorkflowPid(upload.getUploadId(), container.getPid(), job.getPid());
        return applicationId;
    }

    private UploadConfig generateUploadConfig(CustomerSpace customerSpace, DCPImportRequest importRequest) {
        UploadConfig uploadConfig = new UploadConfig();
        if (StringUtils.isNotEmpty(importRequest.getFileImportId())) {
            SourceFile sourceFile = sourceFileProxy.findByName(customerSpace.toString(),
                    importRequest.getFileImportId());
            if (sourceFile == null || StringUtils.isEmpty(sourceFile.getPath())) {
                throw new IllegalArgumentException(String.format("Not a valid source file %s to import!",
                        importRequest.getFileImportId()));
            }
            uploadConfig.setSourceOnHdfs(Boolean.TRUE);
            uploadConfig.setDropFilePath(sourceFile.getPath());
        } else {
            uploadConfig.setDropFilePath(importRequest.getS3FileKey());
        }
        return uploadConfig;
    }

    private DCPSourceImportWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace, String projectId,
                                                                       String sourceId, String uploadId, long statsId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        return new DCPSourceImportWorkflowConfiguration.Builder()
                .customer(customerSpace) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .userId(DEFAULT_DCP_S3_USER) //
                .projectId(projectId) //
                .sourceId(sourceId) //
                .uploadId(uploadId) //
                .statsPid(statsId) //
                .inputProperties(ImmutableMap.<String, String>builder()
                        .put(DCPSourceImportWorkflowConfiguration.UPLOAD_ID, uploadId) //
                        .put(DCPSourceImportWorkflowConfiguration.SOURCE_ID, sourceId) //
                        .put(DCPSourceImportWorkflowConfiguration.PROJECT_ID, projectId)
                        .build()) //
                .matchConfig(getMatchConfig(customerSpace.toString(), sourceId)) //
                .build();
    }

    private DplusMatchConfig getMatchConfig(String customerSpace, String sourceId) {
        MatchRuleConfiguration matchRuleConfiguration = matchRuleService.getMatchConfig(customerSpace, sourceId);
        if(matchRuleConfiguration == null) {
            DplusMatchRule baseRule = new DplusMatchRule(7, Collections.singleton(".*A.*")).exclude(OutOfBusiness);
            return new DplusMatchConfig(baseRule);
        }

        DplusMatchRule baseRule = new DplusMatchRule();
        baseRule.setAcceptCriterion(matchRuleConfiguration.getBaseRule().getAcceptCriterion());
        baseRule.setExclusionCriteria(matchRuleConfiguration.getBaseRule().getExclusionCriterionList());
        baseRule.setReviewCriterion(matchRuleConfiguration.getBaseRule().getReviewCriterion());

        DplusMatchConfig dplusMatchConfig =  new DplusMatchConfig(baseRule);

        matchRuleConfiguration.getSpecialRules().stream().forEach(matchRule -> {
            DplusMatchConfig.SpeicalRule specialRule = new DplusMatchConfig.SpeicalRule();
            specialRule.setMatchKey(matchRule.getMatchKey());
            specialRule.setAllowedValues(matchRule.getAllowedValues());
            if(matchRule.getAcceptCriterion() != null || matchRule.getExclusionCriterionList() != null ||
                    matchRule.getReviewCriterion() != null){
                DplusMatchRule rule = new DplusMatchRule();
                rule.setAcceptCriterion(matchRule.getAcceptCriterion());
                rule.setExclusionCriteria(matchRule.getExclusionCriterionList());
                rule.setReviewCriterion(matchRule.getReviewCriterion());
                specialRule.setSpecialRule(rule);
            }
            dplusMatchConfig.addSpecialRule(specialRule);
        });
        return dplusMatchConfig;
    }

}
