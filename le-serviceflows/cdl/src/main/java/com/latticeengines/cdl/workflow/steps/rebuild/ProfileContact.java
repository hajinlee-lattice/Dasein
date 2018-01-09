package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateReportConfig;
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

    private List<String> dedupFields = ImmutableList.of(InterfaceName.AccountId.name());

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

        TransformationStepConfig profile = profile(masterTableName);
        TransformationStepConfig bucket = bucket(profileStep, masterTableName);
        TransformationStepConfig calc = calcStats(profileStep, bucketStep, statsTablePrefix, dedupFields);
        TransformationStepConfig sort = sort(bucketStep, servingStoreTablePrefix, servingStoreSortKey, 200);
        TransformationStepConfig sortProfile = sort(profileStep, profileTablePrefix,
                DataCloudConstants.PROFILE_ATTR_ATTRNAME, 1);
        // -----------
        List<TransformationStepConfig> steps = Arrays.asList( //
                profile, //
                bucket, //
                calc, //
                sort, //
                sortProfile //
        );
        // if (StringUtils.isNotBlank(accountMasterTableName)) {
        // TransformationStepConfig report = report();
        // steps = new ArrayList<>(steps);
        // steps.add(report);
        // }
        // -----------
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig report() {
        TransformationStepConfig step = new TransformationStepConfig();

        String masterTableSourceName = "ContactUniverse";
        SourceTable sourceTable1 = new SourceTable(masterTableName, customerSpace);
        String accountTableSourceName = "AccountUniverse";
        SourceTable sourceTable2 = new SourceTable(accountMasterTableName, customerSpace);
        List<String> baseSources = Arrays.asList(masterTableSourceName, accountTableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = ImmutableMap.of(masterTableSourceName, sourceTable1, //
                accountTableSourceName, sourceTable2);
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
        Map<String, Attribute> masterAttrs = new HashMap<>();
        masterTable.getAttributes().forEach(attr -> {
            masterAttrs.put(attr.getName(), attr);
        });
        List<Attribute> attrs = new ArrayList<>();
        final AtomicLong masterCount = new AtomicLong(0);
        table.getAttributes().forEach(attr0 -> {
            Attribute attr = copyMasterAttr(masterAttrs, attr0);
            if (masterAttrs.containsKey(attr0.getName())) {
                attr = copyMasterAttr(masterAttrs, attr0);
                masterCount.incrementAndGet();
            }
            attr.setCategory(Category.CONTACT_ATTRIBUTES);
            attr.removeAllowedDisplayNames();
        });
        table.setAttributes(attrs);
        log.info("Copied " + masterCount.get() + " attributes from batch store metadata.");
    }

}
