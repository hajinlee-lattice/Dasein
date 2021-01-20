package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.LaunchBaseType;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LiveRampChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.ImportDeltaCalculationResultsFromS3StepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importDeltaCalculationResultsFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDeltaCalculationResultsFromS3
        extends BaseImportExportS3<ImportDeltaCalculationResultsFromS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportDeltaCalculationResultsFromS3.class);

    @Inject
    private PlayProxy playProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String playId = configuration.getPlayId();
        String launchId = configuration.getLaunchId();
        log.info(String.format("Building requests for tenant=%s, playId=%s launchId=%s", customerSpace.getTenantId(),
                playId, launchId));
        List<String> tables = getMetadataTableNames(customerSpace, playId, launchId);
        if (CollectionUtils.isEmpty(tables)) {
            log.error(String.format("There is no metadata tables associated with tenant %s", customerSpace.getTenantId()));
        }
    }

    protected List<String> getMetadataTableNames(CustomerSpace customerSpace, String playId, String launchId) {
        PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace.getTenantId(), playId, launchId);
        if (playLaunch == null) {
            throw new NullPointerException("PlayLaunch should not be null");
        }
        ChannelConfig channelConfig = playLaunch.getChannelConfig();
        if (channelConfig == null) {
            throw new NullPointerException("ChannelConfig should not be null");
        }

        List<String> tableNames = new ArrayList<>();
        int totalDfs = 0;
        String addAccounts = playLaunch.getAddAccountsTable();
        String addContacts = playLaunch.getAddContactsTable();
        String delAccounts = playLaunch.getRemoveAccountsTable();
        String delContacts = playLaunch.getRemoveContactsTable();
        String completeContacts = playLaunch.getCompleteContactsTable();
        AudienceType audienceType = channelConfig.getAudienceType();
        CDLExternalSystemName systemName = playLaunch.getDestinationSysName();
        boolean launchToDb = CDLExternalSystemName.Salesforce.equals(systemName) || CDLExternalSystemName.Eloqua.equals(systemName);
        boolean isLiveRampLaunch = channelConfig instanceof LiveRampChannelConfig;
        boolean hasOutreachTaskDescription = hasOutreachTaskDescription(channelConfig);

        if (isLiveRampLaunch) {
            if (StringUtils.isNotEmpty(addContacts)) {
                totalDfs += 1;
                putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME,
                        Boolean.toString(true));
                tableNames.add(addContacts);
            }
            if (StringUtils.isNotEmpty(delContacts)) {
                totalDfs += 1;
                putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME,
                        Boolean.toString(true));
                tableNames.add(delContacts);
            }
        } else if (AudienceType.ACCOUNTS == audienceType) {
            if (StringUtils.isNotEmpty(addAccounts)) {
                totalDfs += 2; // add csv and recommendation csv
                putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_RECOMMENDATION_DATA_FRAME,
                        Boolean.toString(true));
                putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME,
                        Boolean.toString(true));
                tableNames.add(addAccounts);
                if (StringUtils.isNotEmpty(addContacts)) {
                    tableNames.add(addContacts);
                }
                if (StringUtils.isNotEmpty(completeContacts)) {
                    tableNames.add(completeContacts);
                }
            }
            if (StringUtils.isNotEmpty(delAccounts) && !launchToDb) {
                totalDfs += 1; // delete csv
                putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME,
                        Boolean.toString(true));
                tableNames.add(delAccounts);
                if (StringUtils.isNotEmpty(delContacts)) {
                    tableNames.add(delContacts);
                }
            }
        } else if (AudienceType.CONTACTS == audienceType) {
            if (StringUtils.isNotEmpty(addContacts)) {
                if (StringUtils.isNotEmpty(addAccounts) && StringUtils.isNotEmpty(completeContacts)) {
                    totalDfs += 2; // add csv and recommendation csv
                    putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_RECOMMENDATION_DATA_FRAME,
                            Boolean.toString(true));
                    putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME,
                            Boolean.toString(true));
                    tableNames.add(addContacts);
                    tableNames.add(addAccounts);
                    tableNames.add(completeContacts);
                } else {
                    throw new RuntimeException("Wrong dataframe combinations for " + addContacts);
                }
            }
            if (StringUtils.isNotEmpty(delContacts) && !launchToDb) {
                if (StringUtils.isNotEmpty(delAccounts)) {
                    totalDfs += 1;
                    putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME,
                            Boolean.toString(true));
                    tableNames.add(delContacts);
                    tableNames.add(delAccounts);
                } else {
                    throw new RuntimeException("Wrong dataframe combinations for " + delContacts);
                }
            }
        } else {
            throw new RuntimeException(audienceType + " not supported.");
        }

        if (hasOutreachTaskDescription) {
            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_TASK_DESCRIPTION_FILE,
                    Boolean.toString(true));
        }

        log.info(String.format("totalDfs=%d, tableNames=%s", totalDfs, Arrays.toString(tableNames.toArray())));
        putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.DATA_FRAME_NUM, String.valueOf(totalDfs));
        if (totalDfs == 0) {
            throw new RuntimeException("There is nothing to be launched.");
        }

        return tableNames;
    }

    protected boolean hasOutreachTaskDescription(ChannelConfig channelConfig) {
        if (channelConfig instanceof OutreachChannelConfig) {
            OutreachChannelConfig outreachConfig = (OutreachChannelConfig) channelConfig;
            String taskDescription = outreachConfig.getTaskDescription();
            return outreachConfig.getLaunchBaseType() == LaunchBaseType.TASK && !StringUtils.isEmpty(taskDescription);
        } else {
            return false;
        }
    }
}
