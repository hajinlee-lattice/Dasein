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
import com.latticeengines.cdl.workflow.steps.campaign.utils.CampaignLaunchUtils;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchProcessor;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchProcessor.ProcessedFieldMappingMetadata;
import com.latticeengines.cdl.workflow.steps.play.FrontEndQueryCreator;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.DeltaCampaignLaunchSparkContext;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
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

    @Value("${datadb.datasource.driver}")
    private String dataDbDriver;

    @Value("${datadb.datasource.sqoop.url}")
    private String dataDbUrl;

    @Value("${datadb.datasource.user}")
    private String dataDbUser;

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;

    @Inject
    private CampaignLaunchUtils campaignLaunchUtils;

    @Inject
    private PlayProxy playProxy;

    private PlayLaunchContext playLaunchContext;

    private boolean createRecommendationDataFrame;

    private boolean createAddCsvDataFrame;

    private boolean createDeleteCsvDataFrame;

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
        playLaunchContext = campaignLaunchProcessor.initPlayLaunchContext(tenant, config);
        setCustomDisplayNames(playLaunchContext);

        ProcessedFieldMappingMetadata processedFieldMappingMetadata = new ProcessedFieldMappingMetadata();
        frontEndQueryCreator.processFieldMappingMetadataWithExistingRecommendationColumns(
                playLaunchContext.getFieldMappingMetadata(), processedFieldMappingMetadata);

        PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();
        log.info("PlayLaunch=" + JsonUtils.serialize(playLaunch));
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
        deltaCampaignLaunchSparkContext.setPublishRecommendationsToDB(campaignLaunchUtils
                .shouldPublishRecommendationsToDB(customerSpace, playLaunch.getDestinationSysName()));
        String saltHint = CipherUtils.generateKey();
        deltaCampaignLaunchSparkContext.setSaltHint(saltHint);
        String encryptionKey = CipherUtils.generateKey();
        deltaCampaignLaunchSparkContext.setEncryptionKey(encryptionKey);
        deltaCampaignLaunchSparkContext.setDataDbPassword(CipherUtils.encrypt(dataDbPassword, encryptionKey, saltHint));

        createRecommendationDataFrame = Boolean.toString(true).equals(
                getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_RECOMMENDATION_DATA_FRAME));
        deltaCampaignLaunchSparkContext.setCreateRecommendationDataFrame(createRecommendationDataFrame);
        createAddCsvDataFrame = Boolean.toString(true)
                .equals(getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        deltaCampaignLaunchSparkContext.setCreateAddCsvDataFrame(createAddCsvDataFrame);
        createDeleteCsvDataFrame = Boolean.toString(true).equals(
                getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));
        deltaCampaignLaunchSparkContext.setCreateDeleteCsvDataFrame(createDeleteCsvDataFrame);
        sparkConfig.setDeltaCampaignLaunchSparkContext(deltaCampaignLaunchSparkContext);
        log.info("sparkConfig=" + JsonUtils.serialize(sparkConfig));
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
        // TODO define the launched account and contacts in the context of delta
        DeltaCampaignLaunchInitStepConfiguration config = getConfiguration();
        int resultDataFrameNum = result.getTargets().size();
        log.info("resultDataFrameNum=" + resultDataFrameNum);
        log.info(result.getOutput());
        long totalAccountsAvailableForLaunch = playLaunchContext.getPlayLaunch().getAccountsSelected();
        long totalContactsAvailableForLaunch = playLaunchContext.getPlayLaunch().getContactsSelected();
        log.info(String.format("Total available accounts available for Launch: %d, contacts: %d",
                totalAccountsAvailableForLaunch, totalContactsAvailableForLaunch));
        long launchedAccountNum = 0L;
        long launchedContactNum = 0L;
        String primaryKey = getPrimaryKey();
        if (createAddCsvDataFrame && !createDeleteCsvDataFrame) {
            String recommendationTargetPath = result.getTargets().get(0).getPath();
            log.info("recommendationTargetPath: " + recommendationTargetPath);
            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(recommendationTargetPath));
            HdfsDataUnit addRecommendationDataUnit = result.getTargets().get(1);
            String addCsvTargetPath = addRecommendationDataUnit.getPath();
            log.info("addCsvTargetPath: " + addCsvTargetPath);
            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(addCsvTargetPath));

            launchedAccountNum = addRecommendationDataUnit.getCount();
            // return a string of array.
            // the first element is the contact num for add csv
            launchedContactNum = JsonUtils
                    .convertList(JsonUtils.deserialize(result.getOutput(), List.class), Long.class).get(0);
            processHDFSDataUnit(String.format("AddedRecommendations_%s", config.getExecutionId()), addRecommendationDataUnit, primaryKey, ADDED_RECOMMENDATION_TABLE);
        } else if (createAddCsvDataFrame && createDeleteCsvDataFrame) {
            String recommendationTargetPath = result.getTargets().get(0).getPath();
            log.info("recommendationTargetPath: " + recommendationTargetPath);
            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(recommendationTargetPath));
            HdfsDataUnit addRecommendationDataUnit = result.getTargets().get(1);
            String addCsvTargetPath = addRecommendationDataUnit.getPath();
            log.info("addCsvTargetPath: " + addCsvTargetPath);
            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(addCsvTargetPath));
            HdfsDataUnit deleteRecommendationDataUnit = result.getTargets().get(2);
            String deleteCsvTargetPath = deleteRecommendationDataUnit.getPath();
            log.info("deleteCsvTargetPath: " + deleteCsvTargetPath);
            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(deleteCsvTargetPath));

            launchedAccountNum = addRecommendationDataUnit.getCount();
            // return a string of array.
            // the first element is the contact num for add csv
            launchedContactNum = JsonUtils
                    .convertList(JsonUtils.deserialize(result.getOutput(), List.class), Long.class).get(0);
            processHDFSDataUnit(String.format("AddedRecommendations_%s", config.getExecutionId()), addRecommendationDataUnit, primaryKey, ADDED_RECOMMENDATION_TABLE);
            processHDFSDataUnit(String.format("DeletedRecommendations_%s", config.getExecutionId()), deleteRecommendationDataUnit, primaryKey, DELETED_RECOMMENDATION_TABLE);
        } else if (!createAddCsvDataFrame && createDeleteCsvDataFrame) {
            HdfsDataUnit deleteRecommendationDataUnit = result.getTargets().get(0);
            String deleteCsvTargetPath = deleteRecommendationDataUnit.getPath();
            log.info("deleteCsvTargetPath: " + deleteCsvTargetPath);
            putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(deleteCsvTargetPath));
            processHDFSDataUnit(String.format("DeletedRecommendations_%s", config.getExecutionId()), deleteRecommendationDataUnit, primaryKey, DELETED_RECOMMENDATION_TABLE);
        } else {
            throw new LedpException(LedpCode.LEDP_70000);
        }
        playProxy.updatePlayLaunch(customerSpace.getTenantId(), playLaunchContext.getPlayName(),
                playLaunchContext.getPlayLaunchId(), playLaunchContext.getPlayLaunch());
        long suppressedAccounts = (totalAccountsAvailableForLaunch - launchedAccountNum);
        long suppressedContacts = (totalContactsAvailableForLaunch - launchedContactNum);
        log.info(String.format("Total suppressed account count for launch: %d", suppressedAccounts));
        log.info(String.format("Total suppressed contact count for launch: %d", suppressedContacts));
    }

    private String getPrimaryKey() {
        AudienceType audienceType = playLaunchContext.getChannel().getChannelConfig().getAudienceType();
        if (audienceType.equals(AudienceType.ACCOUNTS)) {
            return InterfaceName.AccountId.name();
        } else {
            return DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + InterfaceName.ContactId.name();
        }
    }

    private void processHDFSDataUnit(String tableName, HdfsDataUnit dataUnit, String primaryKey, String tableNameKey) {
        Table dataUnitTable = toTable(tableName, primaryKey, dataUnit);
        metadataProxy.createTable(customerSpace.getTenantId(), dataUnitTable.getName(), dataUnitTable);
        PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();
        String metadataTableName = dataUnitTable.getName();
        putObjectInContext(tableNameKey, metadataTableName);
        switch (tableNameKey) {
            case ADDED_RECOMMENDATION_TABLE:
                playLaunch.setAddRecommendationsTable(metadataTableName);
                break;
            case DELETED_RECOMMENDATION_TABLE:
                playLaunch.setDeleteRecommendationsTable(metadataTableName);
                break;
            default:
                log.info("Will not update play launch data.");
        }
        log.info(String.format("Created table %s.", tableName));
    }
}
