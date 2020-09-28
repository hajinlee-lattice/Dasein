package com.latticeengines.cdl.workflow.steps.play;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.LiveRampCampaignLaunchInitStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("liveRampCampaignLaunchInitStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LiveRampCampaignLaunchInitStep
        extends BaseWorkflowStep<LiveRampCampaignLaunchInitStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(LiveRampCampaignLaunchInitStep.class);

    public static final String RECORD_ID_DISPLAY_NAME = "Record ID";

    @Inject
    private PlayProxy playProxy;

    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        LiveRampCampaignLaunchInitStepConfiguration config = getConfiguration();
        customerSpace = config.getCustomerSpace();

        putLiveRampDisplayNamesInContext();
        putPlayLaunchAvroPathsInContext(getPlayLaunchFromConfiguration());
    }

    protected PlayLaunch getPlayLaunchFromConfiguration() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String playName = configuration.getPlayName();
        String launchId = configuration.getPlayLaunchId();

        return playProxy.getPlayLaunch(customerSpace.getTenantId(), playName, launchId);
    }

    protected void putLiveRampDisplayNamesInContext() {
        Map<String, String> contactDisplayNames = new HashMap<>();
        contactDisplayNames.put(ContactMasterConstants.TPS_ATTR_RECORD_ID, RECORD_ID_DISPLAY_NAME);
        
        putObjectInContext(RECOMMENDATION_CONTACT_DISPLAY_NAMES, contactDisplayNames);
    }

    protected void putPlayLaunchAvroPathsInContext(PlayLaunch playLaunch) {
        boolean createAddCsvDataFrame = Boolean.toString(true)
                .equals(getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        boolean createDeleteCsvDataFrame = Boolean.toString(true).equals(
                getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));
        
        if (createAddCsvDataFrame) {
            HdfsDataUnit addCsvDataUnit = getHdfsDataUnitFromTable(playLaunch.getAddContactsTable());

            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(addCsvDataUnit.getPath()));
        }

        if (createDeleteCsvDataFrame) {
            HdfsDataUnit deleteCsvDataUnit = getHdfsDataUnitFromTable(playLaunch.getRemoveContactsTable());

            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(deleteCsvDataUnit.getPath()));
        }
        
        if (!createAddCsvDataFrame && !createDeleteCsvDataFrame) {
            throw new LedpException(LedpCode.LEDP_70000);
        }
    }

    protected HdfsDataUnit getHdfsDataUnitFromTable(String tableName) {
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        if (table == null) {
            throw new RuntimeException("Table " + tableName + " for customer " //
                    + CustomerSpace.shortenCustomerSpace(customerSpace.toString()) //
                    + " does not exists.");
        }
        return table.toHdfsDataUnit(tableName);
    }
}
