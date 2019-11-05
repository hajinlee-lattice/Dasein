package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchProcessor;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchProcessor.ProcessedFieldMappingMetadata;
import com.latticeengines.cdl.workflow.steps.play.FrontEndQueryCreator;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.pls.DeltaCampaignLaunchSparkContext;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CreateDeltaRecommendationConfig;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.CreateDeltaRecommendationsJob;

@Component("deltaCampaignLaunchInitStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeltaCampaignLaunchInitStep
        extends RunSparkJob<DeltaCampaignLaunchInitStepConfiguration, CreateDeltaRecommendationConfig> {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchInitStep.class);

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private CampaignLaunchProcessor campaignLaunchProcessor;

    @Inject
    private FrontEndQueryCreator frontEndQueryCreator;

    @Inject
    private PlayProxy playProxy;

    @Value("${datadb.datasource.driver}")
    private String dataDbDriver;

    @Value("${datadb.datasource.sqoop.url}")
    private String dataDbUrl;

    @Value("${datadb.datasource.user}")
    private String dataDbUser;

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;

    @Override
    protected Class<CreateDeltaRecommendationsJob> getJobClz() {
        return CreateDeltaRecommendationsJob.class;
    }

    @Override
    protected CreateDeltaRecommendationConfig configureJob(DeltaCampaignLaunchInitStepConfiguration stepConfiguration) {
        CreateDeltaRecommendationConfig sparkConfig = new CreateDeltaRecommendationConfig();
        DeltaCampaignLaunchInitStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        log.info("Inside DeltaCampaignLaunchInitStep execute()");
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

        log.info(String.format("For tenant: %s", customerSpace.toString()) + "\n"
                + String.format("For playId: %s", playName) + "\n"
                + String.format("For playLaunchId: %s", playLaunchId));

        // TODO change the context to be more tight with (Delta)CampaignLaunch
        PlayLaunchContext playLaunchContext = campaignLaunchProcessor.initPlayLaunchContext(tenant, config);
        setCustomDisplayNames(playLaunchContext);

        ProcessedFieldMappingMetadata processedFieldMappingMetadata = new ProcessedFieldMappingMetadata();
        frontEndQueryCreator.processFieldMappingMetadataWithExistingRecommendationColumns(
                playLaunchContext.getFieldMappingMetadata(), processedFieldMappingMetadata);

        PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace.getTenantId(), playName, playLaunchId);
        String addAccounts = playLaunch.getAddAccountsTable();
        String addContacts = playLaunch.getAddContactsTable();
        String delAccounts = playLaunch.getRemoveAccountsTable();
        String delContacts = playLaunch.getRemoveContactsTable();
        String completeContacts = playLaunch.getCompleteContactsTable();
        List<String> tableNames = Arrays.asList(addAccounts, addContacts, delAccounts, delContacts, completeContacts);
        List<DataUnit> input = processTableNames(tableNames);
        sparkConfig.setInput(input);

        String totalDfs = getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.DATA_FRAME_NUM);
        log.info("Going to generate " + totalDfs + " dataframes.");
        sparkConfig.setTargetNums(Integer.parseInt(totalDfs));

        DeltaCampaignLaunchSparkContext deltaCampaignLaunchSparkContext = playLaunchContext
                .toDeltaCampaignLaunchSparkContext();
        deltaCampaignLaunchSparkContext
                .setAccountColsRecIncluded(processedFieldMappingMetadata.getAccountColsRecIncluded());
        deltaCampaignLaunchSparkContext
                .setAccountColsRecNotIncludedStd(processedFieldMappingMetadata.getAccountColsRecNotIncludedStd());
        deltaCampaignLaunchSparkContext
                .setAccountColsRecNotIncludedNonStd(processedFieldMappingMetadata.getAccountColsRecNotIncludedNonStd());

        deltaCampaignLaunchSparkContext.setContactCols(processedFieldMappingMetadata.getContactCols());
        deltaCampaignLaunchSparkContext.setDataDbDriver(dataDbDriver);
        deltaCampaignLaunchSparkContext.setDataDbUrl(dataDbUrl);
        deltaCampaignLaunchSparkContext.setDataDbUser(dataDbUser);
        String saltHint = CipherUtils.generateKey();
        deltaCampaignLaunchSparkContext.setSaltHint(saltHint);
        String encryptionKey = CipherUtils.generateKey();
        deltaCampaignLaunchSparkContext.setEncryptionKey(encryptionKey);
        deltaCampaignLaunchSparkContext.setDataDbPassword(CipherUtils.encrypt(dataDbPassword, encryptionKey, saltHint));

        boolean createRecommendationDataFrame = Boolean.toString(true).equals(
                getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_RECOMMENDATION_DATA_FRAME));
        deltaCampaignLaunchSparkContext.setCreateRecommendationDataFrame(createRecommendationDataFrame);
        boolean createAddCsvDataFrame = Boolean.toString(true)
                .equals(getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        deltaCampaignLaunchSparkContext.setCreateAddCsvDataFrame(createAddCsvDataFrame);
        boolean createDeleteCsvDataFrame = Boolean.toString(true).equals(
                getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));
        deltaCampaignLaunchSparkContext.setCreateDeleteCsvDataFrame(createDeleteCsvDataFrame);

        sparkConfig.setDeltaCampaignLaunchSparkContext(deltaCampaignLaunchSparkContext);

        return sparkConfig;
    }

    @VisibleForTesting
    List<DataUnit> processTableNames(List<String> tableNames) {
        return tableNames.stream().map(tableName -> {
            if (tableName == null) {
                return null;
            } else {
                Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
                if (table == null) {
                    throw new RuntimeException("Table " + tableName + " for customer " //
                            + CustomerSpace.shortenCustomerSpace(customerSpace.toString()) //
                            + " does not exists.");
                }
                return table.toHdfsDataUnit(tableName);
            }
        }).collect(Collectors.toList());
    }

    private void successUpdates(CustomerSpace customerSpace, String playName, String playLaunchId) {
        playProxy.updatePlayLaunch(customerSpace.toString(), playName, playLaunchId, LaunchState.Launched);
        playProxy.publishTalkingPoints(customerSpace.toString(), playName);
    }

    private void setCustomDisplayNames(PlayLaunchContext playLaunchContext) {
        List<ColumnMetadata> columnMetadata = playLaunchContext.getFieldMappingMetadata();
        if (CollectionUtils.isNotEmpty(columnMetadata)) {
            Map<String, String> contactDisplayNames = columnMetadata.stream()
                    .filter(col -> BusinessEntity.Contact.equals(col.getEntity()))
                    .collect(Collectors.toMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName));
            Map<String, String> accountDisplayNames = columnMetadata.stream()
                    .filter(col -> !BusinessEntity.Contact.equals(col.getEntity()))
                    .collect(Collectors.toMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName));
            log.info("accountDisplayNames map: " + accountDisplayNames);
            log.info("contactDisplayNames map: " + contactDisplayNames);

            putObjectInContext(RECOMMENDATION_ACCOUNT_DISPLAY_NAMES, accountDisplayNames);
            putObjectInContext(RECOMMENDATION_CONTACT_DISPLAY_NAMES, contactDisplayNames);
        }
    }

    @Override
    protected CustomerSpace parseCustomerSpace(DeltaCampaignLaunchInitStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        // TODO Auto-generated method stub

    }

}
