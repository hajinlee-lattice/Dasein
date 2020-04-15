package com.latticeengines.apps.cdl.workflow;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.CampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("campaignLaunchWorkflowSubmitter")
public class CampaignLaunchWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchWorkflowSubmitter.class);

    @Inject
    private BatonService batonService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @Inject
    private WorkflowProxy workflowProxy;

    public Long submit(PlayLaunch playLaunch) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "campaignLaunchWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.PLAY_NAME, playLaunch.getPlay().getName());
        inputProperties.put(WorkflowContextConstants.Inputs.PLAY_LAUNCH_ID, playLaunch.getLaunchId());
        LookupIdMap lookupIdMap = lookupIdMappingService.getLookupIdMapByOrgId(playLaunch.getDestinationOrgId(),
                playLaunch.getDestinationSysType());

        DataCollection.Version version = dataCollectionService.getActiveVersion(getCustomerSpace().toString());
        CampaignLaunchWorkflowConfiguration configuration = new CampaignLaunchWorkflowConfiguration.Builder()
                .workflow("campaignLaunchWorkflow") //
                .customer(getCustomerSpace()) //
                .inputProperties(inputProperties) //
                .playLaunch(playLaunch) //
                .lookupIdMap(lookupIdMap) //
                .dataCollectionVersion(version) //
                .playLaunchDestination(playLaunch.getDestinationSysType()) //
                .accountAttributeExportDiplayNames(
                        getAccountDisplayNameMap(playLaunch.getDestinationSysType(), lookupIdMap)) //
                .contactAttributeExportDiplayNames(
                        getContactDisplayNameMap(playLaunch.getDestinationSysType(), lookupIdMap)) //
                .exportPublishPlayLaunch(playLaunch, enableExternalLaunch(playLaunch, lookupIdMap)).build();
        ApplicationId appId = workflowJobService.submit(configuration);

        if (FakeApplicationId.isFakeApplicationId(appId.toString())) {
            return FakeApplicationId.toWorkflowJobPid(appId.toString());
        } else {
            Job job = workflowProxy.getWorkflowJobFromApplicationId(appId.toString(), getCustomerSpace().getTenantId());
            return job.getPid();
        }
    }

    private Map<String, String> getContactDisplayNameMap(CDLExternalSystemType destinationSysType,
            LookupIdMap lookupIdMap) {
        if (destinationSysType != CDLExternalSystemType.FILE_SYSTEM && (lookupIdMap.getExternalAuthentication() == null
                || lookupIdMap.getExternalAuthentication().getTrayAuthenticationId() == null)) {
            return null;
        }
        String filePath = lookupIdMap.getExternalSystemName() == CDLExternalSystemName.AWS_S3
                ? "com/latticeengines/cdl/play/launch/s3/contact_display_names.csv"
                : "com/latticeengines/cdl/play/launch/default/contact_display_names.csv";
        return readCsvIntoMap(filePath);
    }

    private Map<String, String> getAccountDisplayNameMap(CDLExternalSystemType destinationSysType,
            LookupIdMap lookupIdMap) {
        if (destinationSysType != CDLExternalSystemType.FILE_SYSTEM && (lookupIdMap.getExternalAuthentication() == null
                || lookupIdMap.getExternalAuthentication().getTrayAuthenticationId() == null)) {
            return null;
        }
        String filePath = lookupIdMap.getExternalSystemName() == CDLExternalSystemName.AWS_S3
                ? "com/latticeengines/cdl/play/launch/s3/account_display_names.csv"
                : "com/latticeengines/cdl/play/launch/default/account_display_names.csv";
        return readCsvIntoMap(filePath);
    }

    private Map<String, String> readCsvIntoMap(String filePath) {
        Map<String, String> map = new HashMap<>();

        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
            String attributeDiplayNames = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
            Scanner scanner = new Scanner(attributeDiplayNames);
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] values = line.split(",");
                map.put(values[0], values[1]);
            }

        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e, new String[] { filePath });
        }
        return map;

    }

    private boolean enableExternalLaunch(PlayLaunch playLaunch, LookupIdMap lookupIdMap) {
        if (StringUtils.isAllBlank(playLaunch.getDestinationOrgId()) || playLaunch.getDestinationSysType() == null) {
            log.info("Skipping Data Export as Destination Org-{} or Destination Type-{} is empty",
                    playLaunch.getDestinationOrgId(), playLaunch.getDestinationSysType());
            return false;
        }

        if (lookupIdMap == null || lookupIdMap.getIsRegistered() == null || !lookupIdMap.getIsRegistered()) {
            log.info("Skipping Data Export as Destination org not found or de-registered - {}",
                    lookupIdMap != null ? lookupIdMap.getIsRegistered() : null);
            return false;
        }
        ExternalSystemAuthentication extSysAuth = lookupIdMap.getExternalAuthentication();
        if (extSysAuth == null || StringUtils.isBlank(extSysAuth.getTrayAuthenticationId())
                || extSysAuth.getTrayWorkflowEnabled() == null || !extSysAuth.getTrayWorkflowEnabled()) {
            log.info("Skipping Data Export as Destination org is not fully configured: {}",
                    extSysAuth != null
                            ? extSysAuth.getTrayAuthenticationId() + "-" + extSysAuth.getTrayWorkflowEnabled()
                            : "Not Configured");
            return false;
        }

        if (lookupIdMap.getExternalSystemName() == CDLExternalSystemName.LinkedIn
                && !batonService.isEnabled(getCustomerSpace(), LatticeFeatureFlag.ENABLE_LINKEDIN_INTEGRATION)) {
            log.info("Skipping Data Export as LinkedIn Integration is disabled for this tenant");
            return false;
        }

        if (lookupIdMap.getExternalSystemName() == CDLExternalSystemName.Facebook
                && !batonService.isEnabled(getCustomerSpace(), LatticeFeatureFlag.ENABLE_FACEBOOK_INTEGRATION)) {
            log.info("Skipping Data Export as Facebook Integration is disabled for this tenant");
            return false;
        }

        return true;
    }
}
