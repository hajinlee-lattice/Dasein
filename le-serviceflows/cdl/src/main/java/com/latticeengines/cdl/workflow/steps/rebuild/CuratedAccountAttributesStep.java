package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_GENERATE_CURATED_ATTRIBUTES;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_NUMBER_OF_CONTACTS;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLCreatedTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLUpdatedTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityCreatedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityLastUpdatedDate;
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
import java.util.Optional;
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
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.NumberOfContactsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(CuratedAccountAttributesStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CuratedAccountAttributesStep extends BaseTransformWrapperStep<CuratedAccountAttributesStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CuratedAccountAttributesStep.class);

    public static final String BEAN_NAME = "curatedAccountAttributesStep";
    public static final String NUMBER_OF_CONTACTS_DISPLAY_NAME = "Number of Contacts";
    private static final String LAST_ACTIVITY_DATE_DISPLAY_NAME = "Lattice Last Activity Date";
    private static final String ENTITY_CREATED_DATE_DISPLAY_NAME = "Lattice Created Date";
    private static final String ENTITY_MODIFIED_DATE_DISPLAY_NAME = "Lattice Last Modified Date";
    private static final String ENTITY_SYS_MODIFIED_DATE_NAME_FMT = "Lattice Last Modified Date by System %s";

    private static final String NUMBER_OF_CONTACTS_DESCRIPTION = "This curated attribute is calculated by counting the "
            + "number of contacts matching each account";
    private static final String LAST_ACTIVITY_DATE_DESCRIPTION = "Most recent activity date among "
            + "any of the time series data (excluding transactions)";
    private static final String ENTITY_CREATED_DATE_DESCRIPTION = "The date when the account is created";
    private static final String ENTITY_MODIFIED_DATE_DESCRIPTION = "Most recent date when the account is updated";
    private static final String ENTITY_SYS_MODIFIED_DATE_DESC_FMT = "Most recent date when the account is updated by system %s";

    // <TEMPLATE_NAME>__<ATTR>
    private static final String SYSTEM_ATTR_FORMAT = "%s__%s";
    // optional curated attrs might not be re-calculated everytime (need to be
    // copied from old table)
    private static final Set<String> CURATED_ACCOUNT_ATTRIBUTES = Sets.newHashSet(NumberOfContacts.name(),
            LastActivityDate.name());

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected MetadataProxy metadataProxy;

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
        if (request == null) {
            return null;
        } else {
            return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
        }
    }

    @Override
    protected void onPostTransformationCompleted() {
        String servingStoreTableName = TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion);
        log.info("Serving store table name = {}", servingStoreTableName);
        Table servingStoreTable = metadataProxy.getTable(customerSpace.toString(), servingStoreTableName);
        enrichTableSchema(servingStoreTable);
        metadataProxy.updateTable(customerSpace.toString(), servingStoreTableName, servingStoreTable);
        exportToS3AndAddToContext(servingStoreTableName, CURATED_ACCOUNT_SERVING_TABLE_NAME);
    }

    protected void enrichTableSchema(Table servingStoreTable) {
        List<Attribute> attrs = servingStoreTable.getAttributes();
        attrs.forEach(attr -> {
            if (NumberOfContacts.name().equals(attr.getName())) {
                attr.setCategory(Category.CURATED_ACCOUNT_ATTRIBUTES);
                attr.setSubcategory(null);
                attr.setDisplayName(NUMBER_OF_CONTACTS_DISPLAY_NAME);
                attr.setDescription(NUMBER_OF_CONTACTS_DESCRIPTION);
                attr.setFundamentalType(FundamentalType.NUMERIC.getName());
            } else if (LastActivityDate.name().equals(attr.getName())) {
                enrichDateAttribute(attr, LAST_ACTIVITY_DATE_DISPLAY_NAME, LAST_ACTIVITY_DATE_DESCRIPTION);
            } else if (EntityLastUpdatedDate.name().equals(attr.getName())) {
                enrichDateAttribute(attr, ENTITY_MODIFIED_DATE_DISPLAY_NAME, ENTITY_MODIFIED_DATE_DESCRIPTION);
            } else if (EntityCreatedDate.name().equals(attr.getName())) {
                enrichDateAttribute(attr, ENTITY_CREATED_DATE_DISPLAY_NAME, ENTITY_CREATED_DATE_DESCRIPTION);
            } else {
                enrichSystemAttributes(attr);
            }
        });
    }

    private void enrichSystemAttributes(@NotNull Attribute attribute) {
        if (StringUtils.isBlank(attribute.getName())) {
            return;
        }

        Optional<String> matchingTmpl = templateSystemMap.keySet() //
                .stream() //
                .filter(tmpl -> {
                    String attr = String.format(SYSTEM_ATTR_FORMAT, tmpl, EntityLastUpdatedDate.name());
                    return attr.equals(attribute.getName());
                }) //
                .findFirst();
        matchingTmpl.ifPresent(tmpl -> {
            String system = templateSystemMap.get(tmpl);
            if (!systemMap.containsKey(system)) {
                log.warn("No corresponding system found for template {}, system {} in attribute {}", tmpl, system,
                        attribute.getName());
                return;
            }

            S3ImportSystem sys = systemMap.get(system);
            if (sys == null || StringUtils.isBlank(sys.getDisplayName())) {
                log.warn("No display name found for system {}", system);
                return;
            }
            String displayName = String.format(ENTITY_SYS_MODIFIED_DATE_NAME_FMT, sys.getDisplayName());
            String description = String.format(ENTITY_SYS_MODIFIED_DATE_DESC_FMT, sys.getDisplayName());
            log.info("Enriching system attribute {} with system display name {}. system = {}, template = {}",
                    attribute.getName(), sys.getDisplayName(), system, tmpl);
            enrichDateAttribute(attribute, displayName, description);
        });
    }

    private void enrichDateAttribute(@NotNull Attribute attribute, @NotNull String displayName,
            @NotNull String description) {
        attribute.setCategory(Category.CURATED_ACCOUNT_ATTRIBUTES);
        attribute.setSubcategory(null);
        attribute.setDisplayName(displayName);
        attribute.setDescription(description);
        attribute.setLogicalDataType(LogicalDataType.Date);
        attribute.setFundamentalType(FundamentalType.DATE.getName());
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
        GenerateCuratedAttributesConfig config = new GenerateCuratedAttributesConfig();
        step.setTransformer(TRANSFORMER_GENERATE_CURATED_ATTRIBUTES);

        int inputIdx = 0;
        // since it's linked in previous step, check inactive is enough
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), CalculatedCuratedAccountAttribute,
                inactive);
        Set<String> currAttrs = new HashSet<>(currentCuratedAttributes(table));
        if (calculateNumOfContacts) {
            step.setInputSteps(Collections.singletonList(numberOfContactsStep));
            currAttrs.remove(NumberOfContacts.name());
            config.attrsToMerge.put(inputIdx,
                    Collections.singletonMap(NumberOfContacts.name(), NumberOfContacts.name()));
            inputIdx++;
        }
        if (calculateLastActivityDate) {
            config.lastActivityDateInputIdx = inputIdx++;
            addBaseTables(step, getLastActivityDateTableName(BusinessEntity.Account));
            currAttrs.remove(LastActivityDate.name());
        }
        if (calculateSystemLastUpdateTime) {
            addBaseTables(step, accountSystemTableName);
            int systemAccountInputIdx = inputIdx++;
            Map<String, String> lastSystemUpdateAttrs = accountTemplates.stream().distinct().map(template -> {
                // copy last update time to account from this system/template
                String srcAttr = String.format(SYSTEM_ATTR_FORMAT, template, CDLUpdatedTime.name());
                String tgtAttr = String.format(SYSTEM_ATTR_FORMAT, template, EntityLastUpdatedDate.name());
                return Pair.of(srcAttr, tgtAttr);
            }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            log.info("Last updated date attributes from systems = {}", lastSystemUpdateAttrs);
            config.attrsToMerge.put(systemAccountInputIdx, lastSystemUpdateAttrs);
            // only EntityId field guaranteed to exist
            config.joinKeys.put(systemAccountInputIdx, EntityId.name());
            // not copying from exist store
            currAttrs.removeAll(lastSystemUpdateAttrs.values());
        }

        // copy create time & last update time from account batch store
        config.masterTableIdx = inputIdx;
        addBaseTables(step, accountTableName);
        Map<String, String> attrsFromAccountTable = new HashMap<>();
        attrsFromAccountTable.put(CDLCreatedTime.name(), EntityCreatedDate.name());
        attrsFromAccountTable.put(CDLUpdatedTime.name(), EntityLastUpdatedDate.name());
        config.attrsToMerge.put(inputIdx, attrsFromAccountTable);
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
            log.info("In short cut mode, skip generating curated account attribute");
            skipTransformation = true;
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
        skipTransformation = !calculateNumOfContacts && !calculateLastActivityDate
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
        return getLastActivityDateTableName(BusinessEntity.Account) != null;
    }

    /*-
     * only re-calculate when having template and have system store.
     * TODO change to only calculate when there's account imports. copy from existing table otherwise
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
        } else if (MapUtils.emptyIfNull(entityImportsMap).containsKey(BusinessEntity.Account)) {
            log.info("Has account import (size={}), re-calculating system last update time",
                    CollectionUtils.size(entityImportsMap.get(BusinessEntity.Account)));
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
        } else if ((MapUtils.isEmpty(entityImportsMap) || (!entityImportsMap.containsKey(BusinessEntity.Account)
                && !entityImportsMap.containsKey(BusinessEntity.Contact)))
                && (configuration.getRebuild() == null || !configuration.getRebuild())) {
            // Skip this step if there are no newly imported accounts and no newly imported
            // contacts, and force rebuild
            // for BusinessEntity CuratedAccounts is null or has been set to false.
            log.warn("There are no newly imported Account or Contacts. Skip calculation number of contact.");
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
                .filter(attr -> {
                    if (CURATED_ACCOUNT_ATTRIBUTES.contains(attr)) {
                        return true;
                    }

                    // check if this is a system last modified date attribute
                    return accountTemplates.stream()
                            .map(tmpl -> String.format(SYSTEM_ATTR_FORMAT, tmpl, EntityLastUpdatedDate.name()))
                            .anyMatch(attr::equals);
                }) //
                .collect(Collectors.toSet());
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
        Map<BusinessEntity, List> templates = getMapObjectFromContext(CONSOLIDATE_TEMPLATES_IN_ORDER,
                BusinessEntity.class, List.class);
        List<String> templateNames;
        if (MapUtils.isNotEmpty(templates) && templates.containsKey(BusinessEntity.Account)) {
            templateNames = JsonUtils.convertList(templates.get(BusinessEntity.Account), String.class);
        } else {
            templateNames = Collections.emptyList();
        }

        log.info("Account templates = {}", templateNames);
        return templateNames;
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
        return getTableName(BusinessEntity.Account.getBatchStore(), "account batch store");
    }

    private String getContactTableName() {
        return getTableName(BusinessEntity.Contact.getBatchStore(), "contact batch store");
    }

    private String getSystemAccountTableName() {
        return getTableName(BusinessEntity.Account.getSystemBatchStore(), "account system batch store");
    }

    private boolean shouldResetCuratedAttributesContext() {
        // reset CuratedAccount when account is reset
        // TODO handle contact reset (erase number of contacts attribute)
        Set<BusinessEntity> entitySet = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (CollectionUtils.emptyIfNull(entitySet).contains(BusinessEntity.Account)) {
            entitySet.add(BusinessEntity.CuratedAccount);
            putObjectInContext(RESET_ENTITIES, entitySet);
            return true;
        } else {
            return false;
        }
    }
}
