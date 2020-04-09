package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_CURATED_ATTRIBUTES;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_NUMBER_OF_CONTACTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILE_TXMFR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.NumberOfContacts;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedCuratedAccountAttribute;

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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.NumberOfContactsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.stats.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeCuratedAttributesConfig;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

// Description: Runs a Workflow Step to compute "curated" attributes which are derived from other attributes.  At this
//     time the only curated attributes is the Number of Contacts per account.  This computation employs the
//     Transformation framework.
@Component(CuratedAccountAttributesStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CuratedAccountAttributesStep extends BaseSingleEntityProfileStep<CuratedAccountAttributesStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CuratedAccountAttributesStep.class);

    public static final String BEAN_NAME = "curatedAccountAttributesStep";

    public static final String NUMBER_OF_CONTACTS_DISPLAY_NAME = "Number of Contacts";
    public static final String LAST_ACTIVITY_DATE_DISPLAY_NAME = "Lattice Last Activity Date";

    private static final Set<String> CURATED_ACCOUNT_ATTRIBUTES = Sets.newHashSet(NumberOfContacts.name(),
            LastActivityDate.name());

    private int numberOfContactsStep, mergedAttributeStep, profileStep, bucketStep;
    private String accountTableName;
    private String contactTableName;

    // Set to true if either of the Account or Contact table is missing or empty and we should not
    // run this step's transformation.
    private boolean skipTransformation;
    private boolean calculateNumOfContacts;
    private boolean calculateLastActivityDate;

    private boolean shortCut;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.CuratedAccount;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return null;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        // Initially, plan to run this step's transformation.
        skipTransformation = false;

        initializeConfiguration();

        // Only generate a Workflow Configuration if all the necessary input tables are available and the
        // step's configuration.
        if (skipTransformation) {
            return null;
        } else {
            return generateWorkflowConf();
        }
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();

        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpace.toString(), //
                Arrays.asList(CURATED_ACCOUNT_SERVING_TABLE_NAME, CURATED_ACCOUNT_STATS_TABLE_NAME));
        shortCut = tablesInCtx.stream().noneMatch(Objects::isNull);

        if (shortCut) {
            log.info("Found both serving and stats tables in workflow context, going thru short-cut mode.");
            servingStoreTableName = tablesInCtx.get(0).getName();
            statsTableName = tablesInCtx.get(1).getName();

            TableRoleInCollection servingStoreRole = BusinessEntity.CuratedAccount.getServingStore();
            dataCollectionProxy.upsertTable(customerSpace.toString(), servingStoreTableName, //
                    servingStoreRole, inactive);
            exportTableRoleToRedshift(servingStoreTableName, servingStoreRole);
            updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);

            skipTransformation = true;

            finishing();
        } else {
            accountTableName = getAccountTableName();
            contactTableName = getContactTableName();

            // Do not run this step if there is no account table, since accounts are required for this step to be
            // meaningful.
            if (StringUtils.isBlank(accountTableName)) {
                log.warn("Cannot find account master table.  Skipping CuratedAccountAttributes Step.");
                skipTransformation = true;
                return;
            }

            calculateNumOfContacts = shouldCalculateNumberOfContacts();
            calculateLastActivityDate = shouldCalculateLastActivityDate();
            skipTransformation = !calculateNumOfContacts && !calculateLastActivityDate
                    && BooleanUtils.isNotTrue(configuration.getRebuild());

            log.info(
                    "calculateNumOfContacts={}, calculateLastActivityDate={}, skipCuratedAccountCalculation={}, rebuild={}",
                    calculateNumOfContacts, calculateLastActivityDate, skipTransformation, configuration.getRebuild());

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
    }

    private String getAccountTableName() {
        String accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                BusinessEntity.Account.getBatchStore(), inactive);
        if (StringUtils.isBlank(accountTableName)) {
            accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    BusinessEntity.Account.getBatchStore(), active);
            if (StringUtils.isNotBlank(accountTableName)) {
                log.info("Found account batch store in active version " + active);
            }
        } else {
            log.info("Found account batch store in inactive version " + inactive);
        }
        return accountTableName;
    }

    private String getContactTableName() {
        String contactTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                BusinessEntity.Contact.getBatchStore(), inactive);
        if (StringUtils.isBlank(contactTableName)) {
            contactTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    BusinessEntity.Contact.getBatchStore(), active);
            if (StringUtils.isNotBlank(contactTableName)) {
                log.info("Found contact batch store in active version " + active);
            }
        } else {
            log.info("Found contact batch store in inactive version " + inactive);
        }
        return contactTableName;
    }

    private boolean shouldResetCuratedAttributesContext() {
        // if either Account or Contact is reset, should reset CuratedAccount too
        Set<BusinessEntity> entitySet = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (CollectionUtils.isNotEmpty(entitySet) && //
                (entitySet.contains(BusinessEntity.Account) || entitySet.contains(BusinessEntity.Contact))) {
            entitySet.add(BusinessEntity.CuratedAccount);
            putObjectInContext(RESET_ENTITIES, entitySet);
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();

        request.setName("CalculateCuratedAttributes");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();

        int currStep = 0;
        if (calculateNumOfContacts) {
            TransformationStepConfig numberOfContacts = numberOfContacts();
            steps.add(numberOfContacts);
            numberOfContactsStep = currStep++;
        }

        mergedAttributeStep = currStep++;
        profileStep = currStep++;
        bucketStep = currStep;

        // PBS
        TransformationStepConfig mergeAttrs = mergeAttributes();
        TransformationStepConfig profile = profile();
        TransformationStepConfig bucket = bucket();
        TransformationStepConfig calcStats = calcStats();
        steps.add(mergeAttrs);
        steps.add(profile);
        steps.add(bucket);
        steps.add(calcStats);

        // -----------
        request.setSteps(steps);
        return request;
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
        MergeCuratedAttributesConfig config = new MergeCuratedAttributesConfig();
        step.setTransformer(TRANSFORMER_MERGE_CURATED_ATTRIBUTES);

        int inputIdx = 0;
        // since it's linked in previous step, check inactive is enough
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), CalculatedCuratedAccountAttribute,
                inactive);
        Set<String> currAttrs = new HashSet<>(currentCuratedAttributes(table));
        if (calculateNumOfContacts) {
            step.setInputSteps(Collections.singletonList(numberOfContactsStep));
            currAttrs.remove(NumberOfContacts.name());
            config.attrsToMerge.put(inputIdx, Collections.singletonList(NumberOfContacts.name()));
            inputIdx++;
        }
        if (calculateLastActivityDate) {
            config.lastActivityDateInputIdx = inputIdx++;
            config.masterTableIdx = inputIdx++;
            addBaseTables(step, getLastActivityDateTableName(BusinessEntity.Account), accountTableName);
            currAttrs.remove(LastActivityDate.name());
        }
        if (!currAttrs.isEmpty()) {
            log.info("Attributes to copied from existing curated account table = {}", currAttrs);
            addBaseTables(step, table.getName());
            config.attrsToMerge.put(inputIdx, new ArrayList<>(currAttrs));
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

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergedAttributeStep));
        step.setTransformer(TRANSFORMER_PROFILE_TXMFR);
        ProfileJobConfig conf = new ProfileJobConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(profileStep, mergedAttributeStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep, mergedAttributeStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalculateStatsConfig conf = new CalculateStatsConfig();
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
        return step;
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        exportToS3AndAddToContext(servingStoreTableName, CURATED_ACCOUNT_SERVING_TABLE_NAME);
        exportToS3AndAddToContext(statsTableName, CURATED_ACCOUNT_STATS_TABLE_NAME);
    }

    private void finishing() {
        updateDCStatusForCuratedAccountAttributes();
        TableRoleInCollection role = CalculatedCuratedAccountAttribute;
        exportToDynamo(servingStoreTableName, role.getPartitionKey(), role.getRangeKey());
    }

    @Override
    protected void enrichTableSchema(Table servingStoreTable) {
        List<Attribute> attrs = servingStoreTable.getAttributes();
        attrs.forEach(attr -> {
            if (NumberOfContacts.name().equals(attr.getName())) {
                attr.setCategory(Category.CURATED_ACCOUNT_ATTRIBUTES);
                attr.setSubcategory(null);
                attr.setDisplayName(NUMBER_OF_CONTACTS_DISPLAY_NAME);
                attr.setDescription("This curated attribute is calculated by counting the number of contacts " +
                        "matching each account");
                attr.setFundamentalType(FundamentalType.NUMERIC.getName());
            } else if (LastActivityDate.name().equals(attr.getName())) {
                attr.setCategory(Category.CURATED_ACCOUNT_ATTRIBUTES);
                attr.setSubcategory(null);
                attr.setDisplayName(LAST_ACTIVITY_DATE_DISPLAY_NAME);
                attr.setDescription(
                        "Most recent activity date among any of the time series data (excluding transactions)");
                attr.setLogicalDataType(LogicalDataType.Date);
                attr.setFundamentalType(FundamentalType.DATE.getName());
            }
        });
    }

    /*-
     * whether to re-calculate last activity date attribute
     */
    private boolean shouldCalculateLastActivityDate() {
        return getLastActivityDateTableName(BusinessEntity.Account) != null;
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
            log.warn("Cannot find contact master table.  Skipping CuratedAccountAttributes Step.");
            return false;
        } else if ((MapUtils.isEmpty(entityImportsMap) || (!entityImportsMap.containsKey(BusinessEntity.Account)
                && !entityImportsMap.containsKey(BusinessEntity.Contact)))
                && (configuration.getRebuild() == null || !configuration.getRebuild())) {
            // Skip this step if there are no newly imported accounts and no newly imported
            // contacts, and force rebuild
            // for BusinessEntity CuratedAccounts is null or has been set to false.
            log.warn("There are no newly imported Account or Contacts.  Skipping CuratedAccountAttributes Step.");
            return false;
        } else if (shouldResetCuratedAttributesContext()) {
            log.warn("Should reset. Skipping CuratedAccountAttributes Step.");
            return false;
        }
        return true;
    }

    private Set<String> currentCuratedAttributes(Table table) {
        if (table == null) {
            log.info("No existing table found for curated account attributes in inactive version {}", inactive);
            return Collections.emptySet();
        }

        return Arrays.stream(table.getAttributeNames()) //
                .filter(CURATED_ACCOUNT_ATTRIBUTES::contains) //
                .collect(Collectors.toSet());
    }

    protected void updateDCStatusForCuratedAccountAttributes() {
        // Get the data collection status map and set the last data refresh time for Curated Accounts to the more
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
            Long curatedAccountcollectionTime = Long.max(accountCollectionTime, contactCollectionTime);
            if (curatedAccountcollectionTime == 0L) {
                log.error("No Account or Contact DataCollectionStatus dates despite running Curated Account "
                        + "Attributes step");
            } else {
                dateMap.put(Category.CURATED_ACCOUNT_ATTRIBUTES.getName(), curatedAccountcollectionTime);
                putObjectInContext(CDL_COLLECTION_STATUS, status);
            }
        }
    }
}
