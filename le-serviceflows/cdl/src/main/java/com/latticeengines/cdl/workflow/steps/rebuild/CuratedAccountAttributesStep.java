package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.ENABLE_ACCOUNT360;
import static com.latticeengines.domain.exposed.admin.LatticeModule.TalkingPoint;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_GENERATE_CURATED_ATTRIBUTES;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_NUMBER_OF_CONTACTS;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.NumberOfContacts;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedCuratedAccountAttribute;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.CuratedAccount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.util.CuratedAttributeUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.NumberOfContactsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Lazy
@Component(CuratedAccountAttributesStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CuratedAccountAttributesStep extends BaseTransformWrapperStep<CuratedAccountAttributesStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CuratedAccountAttributesStep.class);

    public static final String BEAN_NAME = "curatedAccountAttributesStep";

    private static final String MASTER_STORE_ENTITY = Account.name();

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private BatonService batonService;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private String accountTableName;
    private String accountSystemTableName;
    private String contactTableName;
    private String servingStoreTablePrefix;
    // template name
    private List<String> accountTemplates;
    // template name -> system name
    private Map<String, String> templateSystemMap;
    // system name -> system
    private Map<String, S3ImportSystem> systemMap;

    // Set to true if either of the Account or Contact table is missing or empty and we should not
    // run this step's transformation.
    private boolean skipTransformation;
    private boolean calculateNumOfContacts;
    private boolean calculateLastActivityDate;
    private boolean calculateSystemLastUpdateTime;

    private int numberOfContactsStep;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        if (skipTransformation) {
            return null;
        }

        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String servingStoreTableName = TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion);
        log.info("Serving store table name = {}", servingStoreTableName);
        Table servingStoreTable = metadataProxy.getTable(customerSpace.toString(), servingStoreTableName);
        CuratedAttributeUtils.enrichTableSchema(servingStoreTable, Category.CURATED_ACCOUNT_ATTRIBUTES,
                MASTER_STORE_ENTITY, templateSystemMap, systemMap, null);
        metadataProxy.updateTable(customerSpace.toString(), servingStoreTableName, servingStoreTable);

        // rename table to strip invalid characters
        renameServingStoreTable(servingStoreTable);

        servingStoreTableName = servingStoreTable.getName();
        finishing(servingStoreTableName);
        exportToS3AndAddToContext(servingStoreTableName, CURATED_ACCOUNT_SERVING_TABLE_NAME);
    }

    private PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();

        request.setName("CalculateCuratedAttributes");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();

        if (calculateNumOfContacts) {
            TransformationStepConfig numberOfContacts = numberOfContacts();
            steps.add(numberOfContacts);
            numberOfContactsStep = 0;
        }

        TransformationStepConfig mergeAttrs = mergeAttributes();
        steps.add(mergeAttrs);

        // -----------
        request.setSteps(steps);
        return request;
    }

    private void finishing(String servingStoreTableName) {
        TableRoleInCollection servingRole = CalculatedCuratedAccountAttribute;
        dataCollectionProxy.upsertTable(customerSpace.toString(), servingStoreTableName,
                CalculatedCuratedAccountAttribute, inactive);
        updateDCStatusForCuratedAccountAttributes();
        exportToDynamo(servingStoreTableName, servingRole.getPartitionKey(), servingRole.getRangeKey());
    }

    private TransformationStepConfig numberOfContacts() {
        TransformationStepConfig step = new TransformationStepConfig();
        // Set up the Account and Contact tables as inputs for counting contacts.
        List<String> baseSources = Arrays.asList(accountTableName, contactTableName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        SourceTable accountSourceTable = new SourceTable(accountTableName, customerSpace);
        SourceTable contactSourceTable = new SourceTable(contactTableName, customerSpace);
        baseTables.put(accountTableName, accountSourceTable);
        baseTables.put(contactTableName, contactSourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER_NUMBER_OF_CONTACTS);

        NumberOfContactsConfig conf = new NumberOfContactsConfig();
        conf.setLhsJoinField(InterfaceName.AccountId.name());
        conf.setRhsJoinField(InterfaceName.AccountId.name());

        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private String renameServingStoreTable(Table servingStoreTable) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String prefix = String.join("_", customerSpace.getTenantId(), CuratedAccount.name());
        String cleanName = NamingUtils.timestamp(prefix);
        log.info("Renaming curated account table from {} to {}", servingStoreTable.getName(), cleanName);
        metadataProxy.renameTable(customerSpace.toString(), servingStoreTable.getName(), cleanName);
        servingStoreTable.setName(cleanName);
        return cleanName;
    }

    /*-
     * merge all curated attributes from different sources
     * 1. newly calculated number of contact step
     * 2. newly calculated last activity date table
     * 3. existing curated attribute table
     *
     * TODO skip this step when there's only number of contacts step
     */
    private TransformationStepConfig mergeAttributes() {
        TransformationStepConfig step = new TransformationStepConfig();
        GenerateCuratedAttributesConfig config = new GenerateCuratedAttributesConfig();
        step.setTransformer(TRANSFORMER_GENERATE_CURATED_ATTRIBUTES);

        int inputIdx = 0;
        // since it's linked in previous step, check inactive is enough
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), CalculatedCuratedAccountAttribute,
                inactive);
        Set<String> currAttrs = new HashSet<>(
                CuratedAttributeUtils.currentCuratedAttributes(table, MASTER_STORE_ENTITY, accountTemplates, inactive));
        if (calculateNumOfContacts) {
            step.setInputSteps(Collections.singletonList(numberOfContactsStep));
            currAttrs.remove(NumberOfContacts.name());
            config.attrsToMerge.put(inputIdx,
                    Collections.singletonMap(NumberOfContacts.name(), NumberOfContacts.name()));
            inputIdx++;
        } else if (isResettingEntity(BusinessEntity.Contact)) {
            log.info("Resetting contact, not copying number of contact attribute from active serving table");
            currAttrs.remove(NumberOfContacts.name());
        }
        if (calculateLastActivityDate) {
            config.lastActivityDateInputIdx = inputIdx++;
            addBaseTables(step, getLastActivityDateTableName(Account));
            currAttrs.remove(LastActivityDate.name());
        }
        if (calculateSystemLastUpdateTime) {
            addBaseTables(step, accountSystemTableName);
            int systemAccountInputIdx = inputIdx++;
            CuratedAttributeUtils.copySystemLastUpdateTimeAttrs(config, MASTER_STORE_ENTITY, accountTemplates,
                    systemAccountInputIdx, currAttrs);
        }

        // copy create time & last update time from account batch store
        config.masterTableIdx = inputIdx;
        addBaseTables(step, accountTableName);
        Map<String, DataFeedTask> templateFeedTaskMap = dataFeedProxy
                .getTemplateToDataFeedTaskMap(customerSpace.toString());
        config.templateSystemMap = CuratedAttributeUtils.templateSourceMap(templateSystemMap, systemMap);
        config.templateSystemTypeMap = CuratedAttributeUtils.templateSystemTypeMap(templateSystemMap, systemMap);
        config.templateTypeMap = CuratedAttributeUtils.templateEntityTypeMap(templateFeedTaskMap);
        config.attrsToMerge.put(inputIdx, CuratedAttributeUtils.attrsMergeFromMasterStore(MASTER_STORE_ENTITY));
        inputIdx++;

        if (!currAttrs.isEmpty()) {
            log.info("Attributes to copied from existing curated account table = {}", currAttrs);
            addBaseTables(step, table.getName());
            config.attrsToMerge.put(inputIdx, currAttrs.stream().map(attr -> Pair.of(attr, attr))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        }
        config.joinKey = InterfaceName.AccountId.name();

        // Set up the Curated Attributes table as a new output table indexed by Account
        // ID.
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setPrimaryKey(InterfaceName.AccountId.name());
        step.setTargetTable(targetTable);

        // set config
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        servingStoreTablePrefix = BusinessEntity.CuratedAccount.getServingStore().name();

        if (isShortCutMode()) {
            String servingTableName = getStringValueFromContext(CURATED_ACCOUNT_SERVING_TABLE_NAME);
            log.info("In short cut mode, skip generating curated account attribute. Serving table name = {}",
                    servingTableName);
            skipTransformation = true;
            finishing(servingTableName);
            return;
        }

        accountTableName = getAccountTableName();
        contactTableName = getContactTableName();
        accountSystemTableName = getSystemAccountTableName();
        accountTemplates = getAccountTemplates();
        templateSystemMap = dataFeedProxy.getTemplateToSystemMap(customerSpace.toString());
        systemMap = getSystemMap();

        // Do not run this step if there is no account table, since accounts are
        // required for this step to be
        // meaningful.
        if (StringUtils.isBlank(accountTableName)) {
            log.warn("Cannot find account master table.  Skipping CuratedAccountAttributes Step.");
            skipTransformation = true;
            return;
        } else if (shouldResetCuratedAttributesContext()) {
            log.warn("Should reset. Skipping CuratedAccountAttributes Step.");
            skipTransformation = true;
            return;
        }

        calculateNumOfContacts = shouldCalculateNumberOfContacts();
        calculateLastActivityDate = shouldCalculateLastActivityDate();
        calculateSystemLastUpdateTime = shouldCalculateSystemLastUpdateTime();
        skipTransformation = !calculateNumOfContacts && !calculateLastActivityDate && !calculateSystemLastUpdateTime
                && BooleanUtils.isNotTrue(configuration.getRebuild());

        log.info(
                "calculateNumOfContacts={}, calculateLastActivityDate={}, calculateSystemLastUpdateTime={}, skipCuratedAccountCalculation={}, rebuild={}",
                calculateNumOfContacts, calculateLastActivityDate, calculateSystemLastUpdateTime, skipTransformation,
                configuration.getRebuild());

        if (!skipTransformation) {
            Table accountTable = metadataProxy.getTable(customerSpace.toString(), accountTableName);
            Table contactTable = metadataProxy.getTable(customerSpace.toString(), contactTableName);
            double accSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, accountTable);
            double ctcSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, contactTable);
            int multiplier = ScalingUtils.getMultiplier(Math.max(accSize, ctcSize));
            log.info("Set scalingMultiplier=" + multiplier + " base on account table size=" //
                    + accSize + " gb and contact table size=" + ctcSize + " gb.");
            scalingMultiplier = multiplier;
        }
    }

    private boolean isShortCutMode() {
        Table table = getTableSummaryFromKey(customerSpace.toString(), CURATED_ACCOUNT_SERVING_TABLE_NAME);
        return table != null;
    }

    /*-
     * whether to re-calculate last activity date attribute
     */
    private boolean shouldCalculateLastActivityDate() {
        return getLastActivityDateTableName(Account) != null;
    }

    /*-
     * only re-calculate when having template and have system store.
     */
    private boolean shouldCalculateSystemLastUpdateTime() {
        if (CollectionUtils.isEmpty(accountTemplates) && StringUtils.isBlank(accountSystemTableName)) {
            log.info(
                    "No account templates ({}) and/or account system store ({}), skip calculating system last update time",
                    accountTemplates, accountSystemTableName);
            return false;
        }
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        if (BooleanUtils.isTrue(configuration.getRebuild())) {
            log.info("In rebuild mode for curated account, re-calculating system last update time");
            return true;
        } else if (MapUtils.emptyIfNull(entityImportsMap).containsKey(Account)) {
            log.info("Has account import (size={}), re-calculating system last update time",
                    CollectionUtils.size(entityImportsMap.get(Account)));
            return true;
        }

        log.info("No account change, not calculating system last update time");
        return false;
    }

    private String getLastActivityDateTableName(BusinessEntity entity) {
        if (!hasKeyInContext(LAST_ACTIVITY_DATE_TABLE_NAME)) {
            log.info("No last activity date table names in ctx {}", LAST_ACTIVITY_DATE_TABLE_NAME);
            return null;
        }
        Map<String, String> lastActivityDateTableNames = getMapObjectFromContext(LAST_ACTIVITY_DATE_TABLE_NAME,
                String.class, String.class);
        return lastActivityDateTableNames.get(entity.name());
    }

    /*-
     * whether to re-calculate number of contacts attribute
     */
    private boolean shouldCalculateNumberOfContacts() {
        // Get a map of the imported BusinessEntities.
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        if (StringUtils.isBlank(contactTableName)) {
            // Do not run this step if there is no contact table, since contacts are
            // required for this step to be
            // meaningful.
            log.warn("Cannot find contact master table. Skip calculation number of contact.");
            return false;
        } else if ((MapUtils.isEmpty(entityImportsMap)
                || (!entityImportsMap.containsKey(Account)
                && !entityImportsMap.containsKey(BusinessEntity.Contact)))
                && (configuration.getRebuild() == null || !configuration.getRebuild())) {
            // Skip this step if there are no newly imported accounts and no newly imported
            // contacts, and force rebuild
            // for BusinessEntity CuratedAccounts is null or has been set to false.
            log.warn("There are no newly imported Account or Contacts. Skip calculation number of contact.");
            return false;
        } else if (isResettingEntity(BusinessEntity.Contact)) {
            log.info("Resetting contact, not calculating number of contact attribute");
            return false;
        }
        return true;
    }

    private String getTableName(@NotNull TableRoleInCollection role, @NotNull String name) {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
        if (StringUtils.isBlank(tableName)) {
            tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
            if (StringUtils.isNotBlank(tableName)) {
                log.info("Found {} (role={}) in active version {}", name, role, active);
            }
        } else {
            log.info("Found {} (role={}) in inactive version {}", name, role, inactive);
        }
        return tableName;
    }

    private List<String> getAccountTemplates() {
        return getEntityTemplates(Account);
    }

    private Map<String, S3ImportSystem> getSystemMap() {
        List<S3ImportSystem> systems = cdlProxy.getS3ImportSystemList(customerSpace.toString());
        return CollectionUtils.emptyIfNull(systems) //
                .stream() //
                .filter(Objects::nonNull) //
                .filter(sys -> StringUtils.isNotBlank(sys.getName())) //
                .collect(Collectors.toMap(S3ImportSystem::getName, sys -> sys));
    }

    private String getAccountTableName() {
        return getTableName(Account.getBatchStore(), "account batch store");
    }

    private String getContactTableName() {
        return getTableName(BusinessEntity.Contact.getBatchStore(), "contact batch store");
    }

    private String getSystemAccountTableName() {
        return getTableName(Account.getSystemBatchStore(), "account system batch store");
    }

    private boolean shouldResetCuratedAttributesContext() {
        // reset CuratedAccount when account is reset
        Set<BusinessEntity> entitySet = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (isResettingEntity(Account)) {
            entitySet.add(BusinessEntity.CuratedAccount);
            putObjectInContext(RESET_ENTITIES, entitySet);
            return true;
        } else {
            return false;
        }
    }

    private void updateDCStatusForCuratedAccountAttributes() {
        // Get the data collection status map and set the last data refresh time for
        // Curated Accounts to the more
        // recent of the data collection times of Accounts and Contacts.
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        Map<String, Long> dateMap = status.getDateMap();
        if (MapUtils.isEmpty(dateMap)) {
            log.error("No data in DataCollectionStatus Date Map despite running Curated Account Attributes step");
        } else {
            Long accountCollectionTime = 0L;
            if (dateMap.containsKey(Category.ACCOUNT_ATTRIBUTES.getName())) {
                accountCollectionTime = dateMap.get(Category.ACCOUNT_ATTRIBUTES.getName());
            }
            Long contactCollectionTime = 0L;
            if (dateMap.containsKey(Category.CONTACT_ATTRIBUTES.getName())) {
                contactCollectionTime = dateMap.get(Category.CONTACT_ATTRIBUTES.getName());
            }
            long curatedAccountCollectionTime = Long.max(accountCollectionTime, contactCollectionTime);
            if (curatedAccountCollectionTime == 0L) {
                log.error("No Account or Contact DataCollectionStatus dates despite running Curated Account "
                        + "Attributes step");
            } else {
                dateMap.put(Category.CURATED_ACCOUNT_ATTRIBUTES.getName(), curatedAccountCollectionTime);
                putObjectInContext(CDL_COLLECTION_STATUS, status);
            }
        }
    }

    protected void exportToDynamo(String tableName, String partitionKey, String sortKey) {
        if (shouldPublishDynamo()) {
            String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
            DynamoExportConfig config = new DynamoExportConfig();
            config.setTableName(tableName);
            config.setInputPath(inputPath);
            config.setPartitionKey(partitionKey);
            if (StringUtils.isNotBlank(sortKey)) {
                config.setSortKey(sortKey);
            }
            addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
        }
    }

    protected boolean shouldPublishDynamo() {
        boolean enableTp = batonService.hasModule(customerSpace, TalkingPoint);
        boolean hasAccount360 = batonService.isEnabled(customerSpace, ENABLE_ACCOUNT360);
        return !skipPublishDynamo && (enableTp || hasAccount360);
    }
}
