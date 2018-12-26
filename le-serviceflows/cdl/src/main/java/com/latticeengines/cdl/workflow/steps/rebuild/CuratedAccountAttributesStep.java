package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_NUMBER_OF_CONTACTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.NumberOfContactsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
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

    private int numberOfContactsStep, profileStep, bucketStep;
    private String accountTableName;
    private String contactTableName;
    // Set to true if either of the Account or Contact table is missing or empty and we should not
    // run this step's transformation.
    boolean skipTransformation;

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

        accountTableName = getAccountTableName();
        contactTableName = getContactTableName();

        if (StringUtils.isBlank(accountTableName)) {
            log.warn("Cannot find account master table.");
            skipTransformation = true;
        }

        if (StringUtils.isBlank(contactTableName)) {
            log.warn("Cannot find contact master table.");
            skipTransformation = true;
        }

        if (skipTransformation) {
            log.warn("Resetting Curated Account Attributes Step context.");
            resetCuratedAttributesContext();
        } else {
            Table accountTable = metadataProxy.getTable(customerSpace.toString(), accountTableName);
            Table contactTable = metadataProxy.getTable(customerSpace.toString(), contactTableName);
            long accCnt = ScalingUtils.getTableCount(accountTable);
            long ctcCnt = ScalingUtils.getTableCount(contactTable);
            int multiplier = ScalingUtils.getMultiplier(Math.max(accCnt, ctcCnt));
            if (multiplier > 1) {
                log.info("Set multiplier=" + multiplier + " base on account table count=" //
                        + accCnt + " and contact table count=" + ctcCnt);
                scalingMultiplier = multiplier;
            }
        }
    }

    private String getAccountTableName() {
        // TODO: Change to BusinessEntity.Account.getBatchStore()
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

    protected void resetCuratedAttributesContext() {
        Set<BusinessEntity> entitySet = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (entitySet == null) {
            entitySet = new HashSet<>();
        }
        entitySet.add(BusinessEntity.CuratedAccount);
        putObjectInContext(RESET_ENTITIES, entitySet);
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

        numberOfContactsStep = 0;
        profileStep = 1;
        bucketStep = 2;

        TransformationStepConfig numberOfContacts = numberOfContacts();
        TransformationStepConfig profile = profile();
        TransformationStepConfig bucket = bucket();
        TransformationStepConfig calcStats = calcStats();
        steps.add(numberOfContacts);
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

        // Set up the Curated Attributes table as a new output table indexed by Account ID.
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setPrimaryKey(InterfaceName.AccountId.name());
        step.setTargetTable(targetTable);

        NumberOfContactsConfig conf = new NumberOfContactsConfig();
        conf.setLhsJoinField(InterfaceName.AccountId.name());
        conf.setRhsJoinField(InterfaceName.AccountId.name());

        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(numberOfContactsStep));
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(profileStep, numberOfContactsStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep, numberOfContactsStep));
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
        updateDCStatusForCuratedAccountAttributes();
        registerDynamoExport();
    }

    @Override
    protected void enrichTableSchema(Table servingStoreTable) {
        List<Attribute> attrs = servingStoreTable.getAttributes();
        attrs.forEach(attr -> {
            if (InterfaceName.NumberOfContacts.name().equals(attr.getName())) {
                attr.setDisplayName(NUMBER_OF_CONTACTS_DISPLAY_NAME);
            }
        });
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

    private void registerDynamoExport() {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), getEntity().getServingStore(),
                inactive);
        exportToDynamo(tableName, InterfaceName.AccountId.name(), null);
    }
}
