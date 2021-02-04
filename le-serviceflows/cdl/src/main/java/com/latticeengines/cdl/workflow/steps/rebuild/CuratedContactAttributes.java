package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.ENABLE_ACCOUNT360;
import static com.latticeengines.domain.exposed.admin.LatticeModule.TalkingPoint;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.Collections;
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
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.util.CuratedAttributeUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedContactAttributesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateCuratedAttributes;

@Component(CuratedContactAttributes.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CuratedContactAttributes
        extends RunSparkJob<CuratedContactAttributesStepConfiguration, GenerateCuratedAttributesConfig> {

    private static final Logger log = LoggerFactory.getLogger(CuratedContactAttributes.class);

    public static final String BEAN_NAME = "curatedContactAttributesStep";
    private static final TableRoleInCollection TABLE_ROLE = TableRoleInCollection.CalculatedCuratedContact;
    private static final String MASTER_STORE_ENTITY = Contact.name();

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    private CloneTableService cloneTableService;

    @Inject
    private BatonService batonService;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;

    private DataCollection.Version inactive;
    private DataCollection.Version active;
    private boolean resetCuratedContact;
    private String accountTableName;
    private String contactTableName;
    private String contactSystemTableName;
    private String contactLastActivityTempTableName;
    // template name
    private List<String> contactTemplates;
    // template name -> system name
    private Map<String, String> templateSystemMap;
    // template name -> entity type of this template
    private Map<String, String> templateTypeMap;
    // system name -> system
    private Map<String, S3ImportSystem> systemMap;

    @Override
    protected CustomerSpace parseCustomerSpace(CuratedContactAttributesStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected Class<? extends AbstractSparkJob<GenerateCuratedAttributesConfig>> getJobClz() {
        return GenerateCuratedAttributes.class;
    }

    @Override
    protected GenerateCuratedAttributesConfig configureJob(
            CuratedContactAttributesStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);

        if (isShortCutMode()) {
            String servingTableName = getStringValueFromContext(CURATED_CONTACT_SERVING_TABLE_NAME);
            log.info("In short cut mode, skip generating curated contact attribute. serving table name = {}",
                    servingTableName);
            finishing(servingTableName);
            return null;
        }

        resetCuratedContact = shouldResetCuratedAttributesContext();
        templateSystemMap = dataFeedProxy.getTemplateToSystemMap(customerSpace.toString());
        systemMap = getSystemMap();
        accountTableName = getAccountTableName();
        contactTableName = getContactTableName();
        contactSystemTableName = getContactSystemTableName();
        contactLastActivityTempTableName = getContactLastActivityDateTableName();
        contactTemplates = getEntityTemplates(Contact);

        if (!resetCuratedContact) {
            cloneTableService.linkInactiveTable(TABLE_ROLE);
        }
        if (!shouldExecute()) {
            return null;
        }

        List<DataUnit> inputs = new ArrayList<>();
        Table accountTable = metadataProxy.getTable(customerSpace.getTenantId(), accountTableName);
        Table contactTable = metadataProxy.getTable(customerSpace.getTenantId(), contactTableName);
        inputs.add(contactTable.toHdfsDataUnit("ContactBatchStore"));
        inputs.add(accountTable.toHdfsDataUnit("AccountBatchStore"));
        int inputIdx = 2;

        // since it's linked in previous step, check inactive is enough
        Table curatedAttrTable = dataCollectionProxy.getTable(customerSpace.toString(), TABLE_ROLE, inactive);
        Set<String> currAttrs = new HashSet<>(
                CuratedAttributeUtils.currentCuratedAttributes(curatedAttrTable, MASTER_STORE_ENTITY, contactTemplates,
                        inactive));

        GenerateCuratedAttributesConfig jobConfig = new GenerateCuratedAttributesConfig();
        jobConfig.joinKey = InterfaceName.ContactId.name();
        jobConfig.columnsToIncludeFromMaster = Collections.singletonList(InterfaceName.AccountId.name());
        jobConfig.masterTableIdx = 0;
        Map<String, DataFeedTask> templateFeedTaskMap = dataFeedProxy
                .getTemplateToDataFeedTaskMap(customerSpace.toString());
        templateTypeMap = CuratedAttributeUtils.templateEntityTypeMap(templateFeedTaskMap);
        jobConfig.templateSystemMap = CuratedAttributeUtils.templateSourceMap(templateSystemMap, systemMap);
        jobConfig.templateSystemTypeMap = CuratedAttributeUtils.templateSystemTypeMap(templateSystemMap, systemMap);
        jobConfig.templateTypeMap = templateTypeMap;
        jobConfig.attrsToMerge.put(0, CuratedAttributeUtils.attrsMergeFromMasterStore(MASTER_STORE_ENTITY));

        // remove orphan contacts
        jobConfig.parentMasterTableIdx = 1;
        jobConfig.joinKeys.put(1, AccountId.name());

        if (StringUtils.isNotBlank(contactLastActivityTempTableName)) {
            log.info("Found contact last activity date table = {}, re-calculating LastActivityDate attribute",
                    contactLastActivityTempTableName);
            Table contactLastActivityTempTable = metadataProxy.getTable(customerSpace.getTenantId(),
                    contactLastActivityTempTableName);
            inputs.add(contactLastActivityTempTable.toHdfsDataUnit("ContactLastActivityDate"));
            jobConfig.lastActivityDateInputIdx = inputIdx++;
            currAttrs.remove(LastActivityDate.name());
        }

        if (shouldCalculateSystemLastUpdateTime()) {
            log.info("Calculating last update time for each system. Contact templates = {}", contactTemplates);

            Table contactSystemStoreTable = metadataProxy.getTable(customerSpace.getTenantId(), contactSystemTableName);
            inputs.add(contactSystemStoreTable.toHdfsDataUnit("ContactSystemStore"));
            int systemContactIdx = inputIdx++;
            CuratedAttributeUtils.copySystemLastUpdateTimeAttrs(jobConfig, MASTER_STORE_ENTITY, contactTemplates,
                    systemContactIdx,
                    currAttrs);
        }

        if (!currAttrs.isEmpty()) {
            log.info("Attributes to copied from existing curated contact table = {}", currAttrs);
            inputs.add(curatedAttrTable.toHdfsDataUnit("CuratedContactAttribute"));
            jobConfig.attrsToMerge.put(inputIdx, currAttrs.stream().map(attr -> Pair.of(attr, attr))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        }

        jobConfig.setInput(inputs);

        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        String resultTableName = tenantId + "_" + NamingUtils.timestamp(TABLE_ROLE.name());
        Table resultTable = toTable(resultTableName, InterfaceName.ContactId.name(), result.getTargets().get(0));
        CuratedAttributeUtils.enrichTableSchema(resultTable, Category.CURATED_CONTACT_ATTRIBUTES, MASTER_STORE_ENTITY,
                templateSystemMap, systemMap, templateTypeMap);
        metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);
        finishing(resultTableName);
        exportToS3AndAddToContext(resultTable, CURATED_CONTACT_SERVING_TABLE_NAME);
    }

    private void finishing(String servingStoreTableName) {
        dataCollectionProxy.upsertTable(customerSpace.toString(), servingStoreTableName, TABLE_ROLE, inactive);
        updateLastRefreshDate();
        exportToDynamo(servingStoreTableName, TABLE_ROLE.getPartitionKey(), TABLE_ROLE.getRangeKey());
    }

    private boolean isShortCutMode() {
        Table table = getTableSummaryFromKey(customerSpace.toString(), CURATED_CONTACT_SERVING_TABLE_NAME);
        return table != null;
    }

    private boolean shouldExecute() {
        if (StringUtils.isBlank(accountTableName)) {
            log.info("No account batch store, skip curated contact step");
            return false;
        } else if (StringUtils.isBlank(contactTableName)) {
            log.info("No contact batch store, skip curated contact step");
            return false;
        } else if (resetCuratedContact) {
            log.warn("Resetting curated contact, skip step");
            return false;
        }
        boolean calculateLastActivityDate = StringUtils.isNotBlank(contactLastActivityTempTableName);
        boolean calculateSystemLastUpdateTime = shouldCalculateSystemLastUpdateTime();
        if (!calculateLastActivityDate && BooleanUtils.isNotTrue(configuration.getRebuild())
                && !calculateSystemLastUpdateTime) {
            log.info(
                    "No need to calculating both last activity date and system last update time and not in rebuild mode, skip curated contact step");
            return false;
        }
        return true;
    }

    /*-
     * only re-calculate when having template and have system store.
     */
    private boolean shouldCalculateSystemLastUpdateTime() {
        if (CollectionUtils.isEmpty(contactTemplates) && StringUtils.isBlank(contactSystemTableName)) {
            log.info(
                    "No contact templates ({}) and/or contact system store ({}), skip calculating system last update time",
                    contactTemplates, contactSystemTableName);
            return false;
        }
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        if (BooleanUtils.isTrue(configuration.getRebuild())) {
            log.info("In rebuild mode for curated contact, re-calculating system last update time");
            return true;
        } else if (MapUtils.emptyIfNull(entityImportsMap).containsKey(Contact)) {
            log.info("Has contact import (size={}), re-calculating system last update time",
                    CollectionUtils.size(entityImportsMap.get(Contact)));
            return true;
        }

        log.info("No contact change, not calculating system last update time");
        return false;
    }

    private String getContactLastActivityDateTableName() {
        if (!hasKeyInContext(LAST_ACTIVITY_DATE_TABLE_NAME)) {
            log.info("No last activity date table names in ctx {}", LAST_ACTIVITY_DATE_TABLE_NAME);
            return null;
        }
        Map<String, String> lastActivityDateTableNames = getMapObjectFromContext(LAST_ACTIVITY_DATE_TABLE_NAME,
                String.class, String.class);
        return lastActivityDateTableNames.get(Contact.name());
    }

    protected void exportToDynamo(String tableName, String partitionKey, String sortKey) {
        if (shouldPublishDynamo()) {
            String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
            DynamoExportConfig config = new DynamoExportConfig();
            config.setTableName(tableName);
            config.setInputPath(PathUtils.toAvroGlob(inputPath));
            config.setPartitionKey(partitionKey);
            if (StringUtils.isNotBlank(sortKey)) {
                config.setSortKey(sortKey);
            }
            addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
        }
    }

    private boolean shouldResetCuratedAttributesContext() {
        // reset CuratedContact when contact is reset
        Set<BusinessEntity> entitySet = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (isResettingEntity(Contact)) {
            entitySet.add(BusinessEntity.CuratedContact);
            putObjectInContext(RESET_ENTITIES, entitySet);
            return true;
        } else {
            return false;
        }
    }

    private String getContactSystemTableName() {
        return getTableName(Contact.getSystemBatchStore(), "contact system batch store");
    }

    private String getContactTableName() {
        return getTableName(Contact.getBatchStore(), "contact batch store");
    }

    private String getAccountTableName() {
        return getTableName(Account.getBatchStore(), "account batch store");
    }

    // TODO move duplicate code to common place
    private Map<String, S3ImportSystem> getSystemMap() {
        List<S3ImportSystem> systems = cdlProxy.getS3ImportSystemList(customerSpace.toString());
        return CollectionUtils.emptyIfNull(systems) //
                .stream() //
                .filter(Objects::nonNull) //
                .filter(sys -> StringUtils.isNotBlank(sys.getName())) //
                .collect(Collectors.toMap(S3ImportSystem::getName, sys -> sys));
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

    private void updateLastRefreshDate() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        Map<String, Long> dateMap = status.getDateMap();
        long currTime = System.currentTimeMillis();
        log.info("Updating last refresh date for curated contact attribute to current time {}", currTime);
        dateMap.put(Category.CURATED_CONTACT_ATTRIBUTES.getName(), currTime);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }

    private boolean shouldPublishDynamo() {
        boolean enableTp = batonService.hasModule(customerSpace, TalkingPoint);
        boolean hasAccount360 = batonService.isEnabled(customerSpace, ENABLE_ACCOUNT360);
        return !skipPublishDynamo && (enableTp || hasAccount360);
    }

}
