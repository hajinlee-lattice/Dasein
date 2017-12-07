package com.latticeengines.cdl.workflow.steps.rebuild;


import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component(ProfileContact.BEAN_NAME)
public class ProfileContact extends BaseSingleEntityProfileStep<ProcessContactStepConfiguration> {

    static final String BEAN_NAME = "profileContact";

    private static final Logger log = LoggerFactory.getLogger(ProfileContact.class);

    private static int profileStep;
    private static int bucketStep;

    private String reportTablePrefix;
    private String masterTableName;
    private String accountMasterTableName;

    private String[] dedupFields = { InterfaceName.AccountId.name() };

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        reportTablePrefix = entity.name() + "_Report";
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.ContactProfile;
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        masterTableName = masterTable.getName();
        accountMasterTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedAccount, inactive);

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ProfileContactStep");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);

        profileStep = 0;
        bucketStep = 1;

        TransformationStepConfig profile = profile();
        TransformationStepConfig bucket = bucket();
        TransformationStepConfig calc = calcStats();
        TransformationStepConfig sort = sort();
        TransformationStepConfig sortProfile = sortProfile();
        // -----------
        List<TransformationStepConfig> steps = Arrays.asList( //
                profile, //
                bucket, //
                calc, //
                sort, //
                sortProfile //
        );
//        if (StringUtils.isNotBlank(accountMasterTableName)) {
//            TransformationStepConfig report = report();
//            steps = new ArrayList<>(steps);
//            steps.add(report);
//        }
        // -----------
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(masterTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(profileStep));
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(masterTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalculateStatsConfig conf = new CalculateStatsConfig();
        conf.setDedupFields(Arrays.asList(dedupFields));
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig sort() {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(bucketStep);
        step.setInputSteps(inputSteps);

        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(200);
        conf.setSplittingThreads(maxSplitThreads);
        conf.setSplittingChunkSize(10000L);
        conf.setCompressResult(true);
        // TODO: only support single sort key now
        conf.setSortingField(servingStoreSortKey);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig sortProfile() {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(profileStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(1);
        conf.setCompressResult(true);
        conf.setSortingField(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(profileTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private TransformationStepConfig report() {
        TransformationStepConfig step = new TransformationStepConfig();

        String masterTableSourceName = "ContactUniverse";
        SourceTable sourceTable1 = new SourceTable(masterTableName, customerSpace);
        String accountTableSourceName = "AccountUniverse";
        SourceTable sourceTable2 = new SourceTable(accountMasterTableName, customerSpace);
        List<String> baseSources = Arrays.asList(masterTableSourceName, accountTableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = ImmutableMap.of(
                masterTableSourceName, sourceTable1, //
                accountTableSourceName, sourceTable2
        );
        step.setBaseTables(baseTables);

        step.setTransformer("ConsolidateReporter");
        ConsolidateReportConfig config = new ConsolidateReportConfig();
        config.setEntity(entity);
        String configStr = appendEngineConf(config, lightEngineConfig());
        step.setConfiguration(configStr);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(reportTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        List<Attribute> attrs = table.getAttributes();
        attrs.forEach(attr -> {
            attr.setCategory(Category.CONTACT_ATTRIBUTES);
            attr.removeAllowedDisplayNames();
        });
    }

}
