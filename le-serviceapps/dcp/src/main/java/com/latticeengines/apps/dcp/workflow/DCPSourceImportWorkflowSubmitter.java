package com.latticeengines.apps.dcp.workflow;

import static com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion.OutOfBusiness;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.apps.dcp.service.EntitlementService;
import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusAppendConfig;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchConfig;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.PurposeOfUse;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadDiagnostics;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;

@Component
public class DCPSourceImportWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(DCPSourceImportWorkflowSubmitter.class);

    private static final String DEFAULT_DCP_S3_USER = "Default_DCP_S3_User";
    private static final int MAX_RETRY = 3;

    @Inject
    private UploadService uploadService;

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private MatchRuleService matchRuleService;

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    private EntitlementService entitlementService;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, DCPImportRequest importRequest,
                                WorkflowPidWrapper pidWrapper) {
        String projectId = importRequest.getProjectId();
        String sourceId = importRequest.getSourceId();
        String customerSpaceStr = customerSpace.toString();

        UploadConfig uploadConfig = generateUploadConfig(customerSpace, importRequest);
        UploadDetails upload = uploadService.createUpload(customerSpaceStr, sourceId,
                uploadConfig, importRequest.getUserId());
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRY);
        UploadStatsContainer container = retryTemplate.execute(context -> {
            UploadStatsContainer containerTmp = new UploadStatsContainer();
            containerTmp = uploadService.appendStatistics(upload.getUploadId(), containerTmp);
            return containerTmp;
        });
        Project project = projectService.getProjectByProjectId(customerSpaceStr, projectId);
        if (project == null) {
            throw new IllegalArgumentException("Cannot find project with projectId: " + projectId);
        }
        MatchRuleConfiguration matchRuleConfiguration = matchRuleService.getMatchConfig(customerSpaceStr, sourceId);
        if (!verifyPurposeOfUse(customerSpace, matchRuleConfiguration)) {
            throw new LedpException(LedpCode.LEDP_60011);
        }

        DCPSourceImportWorkflowConfiguration configuration = //
                generateConfiguration(customerSpace, projectId, sourceId, upload.getUploadId(), container.getPid());
        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());
        Job job = workflowJobService.findByApplicationId(applicationId.toString());
        uploadService.updateStatsWorkflowPid(upload.getUploadId(), container.getPid(), job.getPid());
        UploadDiagnostics uploadDiagnostics = new UploadDiagnostics();
        uploadDiagnostics.setApplicationId(applicationId.toString());
        uploadService.updateUploadStatus(customerSpaceStr, upload.getUploadId(), upload.getStatus(), uploadDiagnostics);
        return applicationId;
    }

    private boolean verifyPurposeOfUse(CustomerSpace customerSpace, MatchRuleConfiguration matchRuleConfiguration) {
        if (matchRuleConfiguration == null || matchRuleConfiguration.getBaseRule() == null) {
            return true;
        }
        if (matchRuleConfiguration.getBaseRule().getDomain() != null && matchRuleConfiguration.getBaseRule().getRecordType() != null) {
            return entitlementService.checkEntitledWith(customerSpace.toString(), matchRuleConfiguration.getBaseRule().getDomain(),
                    matchRuleConfiguration.getBaseRule().getRecordType(), DataBlock.BLOCK_COMPANY_ENTITY_RESOLUTION);
        }
        return true;
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

        DplusMatchConfig matchConfig = getMatchConfig(customerSpace.toString(), sourceId);
        PurposeOfUse matchPurpose = getMatchPurpose(customerSpace.toString(), sourceId);
        EnrichmentLayout enrichmentLayout = getEnrichmentLayout(customerSpace.toString(), sourceId);
        DplusAppendConfig appendConfig = new DplusAppendConfig();
        appendConfig.setElementIds(enrichmentLayout.getElements());
        PurposeOfUse appendPurpose = getAppendPurpose(enrichmentLayout);

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
                .matchConfig(matchConfig, matchPurpose) //
                .appendConfig(appendConfig, appendPurpose) //
                .build();
    }

    private PurposeOfUse getAppendPurpose(EnrichmentLayout enrichmentLayout) {
        DataDomain domain = enrichmentLayout.getDomain();
        DataRecordType recordType = enrichmentLayout.getRecordType();
        if (domain == null) {
            return getDefaultPurposeOfUse();
        } else if (recordType == null) {
            recordType = getDefaultRecordType();
        }
        PurposeOfUse purposeOfUse = new PurposeOfUse();
        purposeOfUse.setDomain(domain);
        purposeOfUse.setRecordType(recordType);
        return purposeOfUse;
    }

    private EnrichmentLayout getEnrichmentLayout(String customerSpace, String sourceId) {
        EnrichmentLayout enrichmentLayout = enrichmentLayoutService.findBySourceId(customerSpace, sourceId);
        if (enrichmentLayout == null) {
            log.warn("EnrichmentLayout is missing: tenant={}, source={}.", //
                    CustomerSpace.shortenCustomerSpace(customerSpace), sourceId);
            enrichmentLayout = enrichmentLayoutService.getDefaultLayout();
        }
        return enrichmentLayout;
    }

    private PurposeOfUse getMatchPurpose(String customerSpace, String sourceId) {
        MatchRuleConfiguration matchRuleConfiguration = matchRuleService.getMatchConfig(customerSpace, sourceId);
        PurposeOfUse purposeOfUse;
        if (matchRuleConfiguration != null && matchRuleConfiguration.getBaseRule() != null) {
            DataDomain domain = matchRuleConfiguration.getBaseRule().getDomain();
            DataRecordType recordType = matchRuleConfiguration.getBaseRule().getRecordType();
            if (domain == null && recordType == null) {
                purposeOfUse = getDefaultPurposeOfUse();
                log.warn("Base rule does not have purpose of use: tenant={}, source={}.", //
                        CustomerSpace.shortenCustomerSpace(customerSpace), sourceId);
            } else if (domain == null) {
                purposeOfUse = getDefaultPurposeOfUse();
                log.warn("Base rule does not have data domain: tenant={}, source={}.", //
                        CustomerSpace.shortenCustomerSpace(customerSpace), sourceId);
            } else if (recordType == null) {
                recordType = getDefaultRecordType();
                purposeOfUse = new PurposeOfUse();
                purposeOfUse.setDomain(domain);
                purposeOfUse.setRecordType(recordType);
                log.warn("Base rule does not have data record type: tenant={}, source={}.", //
                        CustomerSpace.shortenCustomerSpace(customerSpace), sourceId);
            } else {
                purposeOfUse = new PurposeOfUse();
                purposeOfUse.setDomain(domain);
                purposeOfUse.setRecordType(recordType);
            }
        } else {
            purposeOfUse = getDefaultPurposeOfUse();
        }

        return purposeOfUse;
    }

    private PurposeOfUse getDefaultPurposeOfUse() {
        PurposeOfUse purposeOfUse = new PurposeOfUse();
        purposeOfUse.setDomain(DataDomain.SalesMarketing);
        purposeOfUse.setRecordType(DataRecordType.Domain);
        return purposeOfUse;
    }

    private DataRecordType getDefaultRecordType() {
        return DataRecordType.Domain;
    }

    private DplusMatchConfig getMatchConfig(String customerSpace, String sourceId) {
        MatchRuleConfiguration matchRuleConfiguration = matchRuleService.getMatchConfig(customerSpace, sourceId);
        if(matchRuleConfiguration == null) {
            DplusMatchRule baseRule = new DplusMatchRule(7, Collections.singleton(".*A.*")).exclude(OutOfBusiness);
            return new DplusMatchConfig(baseRule);
        }

        DplusMatchRule baseRule = new DplusMatchRule();
        if(matchRuleConfiguration.getBaseRule().getAcceptCriterion() != null) {
            baseRule.accept(matchRuleConfiguration.getBaseRule().getAcceptCriterion().getLowestConfidenceCode(),
                    matchRuleConfiguration.getBaseRule().getAcceptCriterion().getHighestConfidenceCode(),
                    matchRuleConfiguration.getBaseRule().getAcceptCriterion().getMatchGradePatterns());
        }
        if(matchRuleConfiguration.getBaseRule().getExclusionCriterionList() != null) {
            matchRuleConfiguration.getBaseRule().getExclusionCriterionList().forEach(baseRule::exclude);
        }
        if(matchRuleConfiguration.getBaseRule().getReviewCriterion() != null) {
            baseRule.review(matchRuleConfiguration.getBaseRule().getReviewCriterion().getLowestConfidenceCode(),
                    matchRuleConfiguration.getBaseRule().getReviewCriterion().getHighestConfidenceCode(),
                    matchRuleConfiguration.getBaseRule().getReviewCriterion().getMatchGradePatterns());
        }

        DplusMatchConfig dplusMatchConfig =  new DplusMatchConfig(baseRule);

        matchRuleConfiguration.getSpecialRules().forEach(matchRule -> {
            DplusMatchRule rule = new DplusMatchRule();
            if(matchRule.getAcceptCriterion() != null) {
                rule.accept(matchRule.getAcceptCriterion().getLowestConfidenceCode(),
                        matchRule.getAcceptCriterion().getHighestConfidenceCode(),
                        matchRule.getAcceptCriterion().getMatchGradePatterns());
            }
            if(matchRule.getExclusionCriterionList() != null){
                matchRule.getExclusionCriterionList().forEach(rule::exclude);
            }
            if(matchRule.getReviewCriterion() != null){
                rule.review(matchRule.getReviewCriterion().getLowestConfidenceCode(),
                        matchRule.getReviewCriterion().getHighestConfidenceCode(),
                        matchRule.getReviewCriterion().getMatchGradePatterns());
            }
            dplusMatchConfig.when(matchRule.getMatchKey(), matchRule.getAllowedValues()).apply(rule);
        });
        return dplusMatchConfig;
    }

}
