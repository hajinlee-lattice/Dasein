package com.latticeengines.cdl.workflow.steps.campaign;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.contactmaster.LiveRampDestination;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.config.TpsMatchConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LiveRampChannelConfig;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.GenerateLiveRampLaunchArtifactStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;

@Component("generateLiveRampLaunchArtifacts")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateLiveRampLaunchArtifacts extends BaseSparkStep<GenerateLiveRampLaunchArtifactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(GenerateLiveRampLaunchArtifacts.class);

    static final Map<CDLExternalSystemName, LiveRampDestination> EXT_SYS_NAME_TO_LIVERAMP_DEST = new HashMap<>();
    static {
        EXT_SYS_NAME_TO_LIVERAMP_DEST.put(CDLExternalSystemName.Adobe_Audience_Mgr, LiveRampDestination.Adobe);
        EXT_SYS_NAME_TO_LIVERAMP_DEST.put(CDLExternalSystemName.AppNexus, LiveRampDestination.AppNexus);
        EXT_SYS_NAME_TO_LIVERAMP_DEST.put(CDLExternalSystemName.Google_Display_N_Video_360, LiveRampDestination.Google);
        EXT_SYS_NAME_TO_LIVERAMP_DEST.put(CDLExternalSystemName.MediaMath, LiveRampDestination.Mediamath);
        EXT_SYS_NAME_TO_LIVERAMP_DEST.put(CDLExternalSystemName.TradeDesk, LiveRampDestination.Tradedesk);
        EXT_SYS_NAME_TO_LIVERAMP_DEST.put(CDLExternalSystemName.Verizon_Media, LiveRampDestination.Verizon);
    }

    private static final String TPS_MATCH_RESULT = "TPSMatchResult";
    private static final String SITE_DUNS = "LDC_DUNS";

    @Inject
    private PlayProxy playProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private BulkMatchService bulkMatchService;

    @Override
    public void execute() {
        GenerateLiveRampLaunchArtifactStepConfiguration config = getConfiguration();
        customerSpace = configuration.getCustomerSpace();

        ChannelConfig channelConfig = getChannelConfig(config);

        if (shouldSkipStep(channelConfig.getSystemName())) {
            return;
        }

        Map<String, Long> newCounts = new HashMap<>();

        String addedAccountsDeltaTable = getStringValueFromContext(ADDED_ACCOUNTS_DELTA_TABLE);
        String removedAccountsDeltaTable = getStringValueFromContext(REMOVED_ACCOUNTS_DELTA_TABLE);

        if (!StringUtils.isEmpty(addedAccountsDeltaTable)) {
            String addedAccountsDeltaAvroPath = getAvroPathFromTable(addedAccountsDeltaTable);

            MatchCommand command = bulkMatchTPS(addedAccountsDeltaAvroPath, channelConfig);

            Long numMatched = Long.valueOf(command.getRowsMatched());

            if (numMatched > 0) {
                HdfsDataUnit addedLiveRampContactsDataUnit = getHdfsDataUnitFromMatchCommand(command);

                processHDFSDataUnit(String.format("AddedContacts_%s", config.getExecutionId()),
                        addedLiveRampContactsDataUnit, ContactMasterConstants.TPS_ATTR_RECORD_ID,
                        ADDED_CONTACTS_DELTA_TABLE);
                
                newCounts.put(ADDED_CONTACTS_DELTA_TABLE, numMatched);
            }

        }

        if (!StringUtils.isEmpty(removedAccountsDeltaTable)) {
            String removedAccountsDeltaAvroPath = getAvroPathFromTable(removedAccountsDeltaTable);

            MatchCommand command = bulkMatchTPS(removedAccountsDeltaAvroPath, channelConfig);

            Long numMatched = Long.valueOf(command.getRowsMatched());

            if (numMatched > 0) {
                HdfsDataUnit removedLiveRampContactsDataUnit = getHdfsDataUnitFromMatchCommand(command);

                processHDFSDataUnit(String.format("RemovedContacts_%s", config.getExecutionId()),
                        removedLiveRampContactsDataUnit, ContactMasterConstants.TPS_ATTR_RECORD_ID,
                        REMOVED_CONTACTS_DELTA_TABLE);

                newCounts.put(REMOVED_CONTACTS_DELTA_TABLE, numMatched);
            }
        }

        log.info("New LiveRamp Contacts Counts: " + JsonUtils.serialize(newCounts));
        putObjectInContext(DELTA_TABLE_COUNTS, newCounts);
    }

    protected ChannelConfig getChannelConfig(GenerateLiveRampLaunchArtifactStepConfiguration config) {
        PlayLaunchChannel channel = playProxy.getChannelById(customerSpace.getTenantId(), config.getPlayId(),
                config.getChannelId());

        return channel.getChannelConfig();
    }

    public boolean shouldSkipStep(CDLExternalSystemName systemName) {
        return !(EXT_SYS_NAME_TO_LIVERAMP_DEST.containsKey(systemName));
    }

    protected String getAvroPathFromTable(String tableName) {
        HdfsDataUnit dataUnit = metadataProxy.getTable(customerSpace.toString(), tableName).toHdfsDataUnit(tableName);
        return dataUnit.getPath();
    }

    protected MatchCommand bulkMatchTPS(String avroDir, ChannelConfig channelConfig) {
        log.info("Starting bulk match");
        MatchInput matchInput = constructMatchInput(avroDir, channelConfig);
        MatchCommand command = bulkMatchService.match(matchInput, null);
        log.info("Bulk match finished: {}", JsonUtils.serialize(command));
        return command;
    }

    protected HdfsDataUnit getHdfsDataUnitFromMatchCommand(MatchCommand command) {
        HdfsDataUnit matchedDataUnit = bulkMatchService.getResultDataUnit(command, TPS_MATCH_RESULT);
        return matchedDataUnit;
    }

    private MatchInput constructMatchInput(String avroDir, ChannelConfig channelConfig) {
        MatchInput matchInput = new MatchInput();
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        matchInput.setOperationalMode(OperationalMode.CONTACT_MATCH);
        matchInput.setTargetEntity(ContactMasterConstants.MATCH_ENTITY_TPS);
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setKeyMap(getKeyMap());
        matchInput.setSkipKeyResolution(true);
        matchInput.setTpsMatchConfig(createTpsMatchConfig(channelConfig));

        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);

        return matchInput;
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.DUNS, Collections.singletonList(SITE_DUNS));
        return keyMap;
    }

    private TpsMatchConfig createTpsMatchConfig(ChannelConfig channelConfig) {
        LiveRampChannelConfig liveRampChannelConfig = (LiveRampChannelConfig) channelConfig;

        TpsMatchConfig matchConfig = new TpsMatchConfig();
        matchConfig.setDestination(
                convertCDLExternalSystemNameToLiveRampDestination(liveRampChannelConfig.getSystemName()));
        matchConfig.setJobFunctions(Arrays.asList(liveRampChannelConfig.getJobFunctions()));
        matchConfig.setJobLevels(Arrays.asList(liveRampChannelConfig.getJobLevels()));

        return matchConfig;
    }

    protected LiveRampDestination convertCDLExternalSystemNameToLiveRampDestination(CDLExternalSystemName systemName) {
        return EXT_SYS_NAME_TO_LIVERAMP_DEST.get(systemName);
    }

    private void processHDFSDataUnit(String tableName, HdfsDataUnit dataUnit, String primaryKey, String contextKey) {
        log.info(getHDFSDataUnitLogEntry(tableName, dataUnit));
        Table dataUnitTable = toTable(tableName, primaryKey, dataUnit);
        metadataProxy.createTable(customerSpace.getTenantId(), dataUnitTable.getName(), dataUnitTable);
        putStringValueInContext(contextKey, tableName);
        log.info("Created " + tableName + " at " + dataUnitTable.getExtracts().get(0).getPath());
    }

    private String getHDFSDataUnitLogEntry(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return tag + " data set empty";
        }
        return tag + ", " + JsonUtils.serialize(dataUnit);
    }

}
