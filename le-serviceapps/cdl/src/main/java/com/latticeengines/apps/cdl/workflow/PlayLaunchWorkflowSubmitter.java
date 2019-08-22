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

import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("playLaunchWorkflowSubmitter")
public class PlayLaunchWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowSubmitter.class);

    @Inject
    private BatonService batonService;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    public ApplicationId submit(PlayLaunch playLaunch) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "playLaunchWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.PLAY_NAME, playLaunch.getPlay().getName());
        inputProperties.put(WorkflowContextConstants.Inputs.PLAY_LAUNCH_ID, playLaunch.getLaunchId());

        LookupIdMap lookupIdMap = lookupIdMappingService.getLookupIdMapByOrgId(playLaunch.getDestinationOrgId(),
                playLaunch.getDestinationSysType());

        boolean enableExport = batonService.isEnabled(getCustomerSpace(),
                LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION)
                || batonService.isEnabled(getCustomerSpace(), LatticeFeatureFlag.ENABLE_FACEBOOK_INTEGRATION)
                || batonService.isEnabled(getCustomerSpace(), LatticeFeatureFlag.ENABLE_LINKEDIN_INTEGRATION);
        boolean canBeLaunchedToExternal = enableExport && isValidDestination(playLaunch, lookupIdMap);

        PlayLaunchWorkflowConfiguration configuration = new PlayLaunchWorkflowConfiguration.Builder()
                .workflow("playLaunchWorkflow").customer(getCustomerSpace()).inputProperties(inputProperties)
                .playLaunch(playLaunch).lookupIdMap(lookupIdMap)
                .playLaunchDestination(playLaunch.getDestinationSysType())
                .accountAttributeExportDiplayNames(getAccountDisplayNameMap(playLaunch.getDestinationSysType()))
                .contactAttributeExportDiplayNames(getContactDisplayNameMap(playLaunch.getDestinationSysType()))
                .exportPublishPlayLaunch(playLaunch, canBeLaunchedToExternal).build();
        return workflowJobService.submit(configuration);
    }

    private Map<String, String> getContactDisplayNameMap(CDLExternalSystemType destinationSysType) {
        if (destinationSysType != CDLExternalSystemType.FILE_SYSTEM) {
            return null;
        }
        return readCsvIntoMap("com/latticeengines/cdl/play/launch/s3/contact_display_names.csv");
    }

    private Map<String, String> getAccountDisplayNameMap(CDLExternalSystemType destinationSysType) {
        if (destinationSysType != CDLExternalSystemType.FILE_SYSTEM) {
            return null;
        }
        return readCsvIntoMap("com/latticeengines/cdl/play/launch/s3/account_display_names.csv");
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

    private boolean isValidDestination(PlayLaunch playLaunch, LookupIdMap lookupIdMap) {
        if (StringUtils.isAllBlank(playLaunch.getDestinationOrgId()) || playLaunch.getDestinationSysType() == null) {
            log.debug("Skipping Data Export as Destination Org-{} or Destination Type-{} is empty",
                    playLaunch.getDestinationOrgId(), playLaunch.getDestinationSysType());
            return false;
        }

        if (lookupIdMap == null || lookupIdMap.getIsRegistered() == null || !lookupIdMap.getIsRegistered()) {
            log.debug("Skipping Data Export as Destination org not found or de-registered - {}",
                    lookupIdMap != null ? lookupIdMap.getIsRegistered() : null);
            return false;
        }
        ExternalSystemAuthentication extSysAuth = lookupIdMap.getExternalAuthentication();
        if (extSysAuth == null || StringUtils.isBlank(extSysAuth.getTrayAuthenticationId())
                || extSysAuth.getTrayWorkflowEnabled() == null || !extSysAuth.getTrayWorkflowEnabled()) {
            log.debug("Skipping Data Export as Destination org is not fully configured: {}",
                    extSysAuth != null
                            ? extSysAuth.getTrayAuthenticationId() + "-" + extSysAuth.getTrayWorkflowEnabled()
                            : "Not Configured");
            return false;
        }
        return true;
    }
}
