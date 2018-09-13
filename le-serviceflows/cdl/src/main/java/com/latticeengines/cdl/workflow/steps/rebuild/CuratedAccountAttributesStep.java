package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_NUMBER_OF_CONTACTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

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
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;

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

    @Inject
    private Configuration yarnConfiguration;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.CuratedAccount;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return null;
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

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();

        accountTableName = getAccountTableName();
        if (StringUtils.isBlank(accountTableName)) {
            throw new IllegalStateException("Cannot find account master table.");
        }

        contactTableName = getContactTableName();
        if (StringUtils.isBlank(contactTableName)) {
            throw new IllegalStateException("Cannot find contact master table.");
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

    // TODO: Not sure if this function is implemented correctly.
    @Override
    protected void enrichTableSchema(Table servingStoreTable) {
        log.error("$JAW$ In CuratedAccountAttributesStep.enrichTablesScheme");

        List<Attribute> attrs = servingStoreTable.getAttributes();
        attrs.forEach(attr -> {
            if (InterfaceName.NumberOfContacts.name().equals(attr.getName())) {
                attr.setDisplayName(NUMBER_OF_CONTACTS_DISPLAY_NAME);
            }
        });

    }

}