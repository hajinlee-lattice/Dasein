package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(ProfileAccount.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileAccount extends ProfileStepBase<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "profileAccount";

    private static final Logger log = LoggerFactory.getLogger(ProfileAccount.class);

    private int profileStep;
    private int bucketStep;

    private String fullAccountTableName;
    private String masterTableName;
    private String statsTableName;
    private String statsTablePrefix = "Stats";

    private DataCollection.Version active;
    private DataCollection.Version inactive;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Account;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        TableRoleInCollection batchStore = BusinessEntity.Account.getBatchStore();
        masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore,
                inactive);
        if (StringUtils.isBlank(masterTableName)) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }

        statsTableName = getStringValueFromContext(ACCOUNT_STATS_TABLE_NAME);
        if (StringUtils.isNotBlank(statsTableName)) {
            Table statsTable = metadataProxy.getTable(customerSpace.toString(), statsTableName);
            if (statsTable != null) {
                log.info("Found stats table in context, going thru short-cut mode.");
                finishing();
                return null;
            }
        }

        // reset result table names
        statsTableName = null;

        fullAccountTableName = getStringValueFromContext(FULL_ACCOUNT_TABLE_NAME);
        if (StringUtils.isBlank(fullAccountTableName)) {
            throw new IllegalStateException("Cannot find the fully enriched account table");
        }
        Table fullAccountTable = metadataProxy.getTable(customerSpace.toString(), fullAccountTableName);
        if (fullAccountTable == null) {
            throw new IllegalStateException("Cannot find the fully enriched account table in default collection");
        }
        long count = ScalingUtils.getTableCount(fullAccountTable);
        int multiplier = ScalingUtils.getMultiplier(count);
        if (multiplier > 1) {
            log.info("Set multiplier=" + multiplier + " base on fully enriched account table count=" + count);
            scalingMultiplier = multiplier;
        }

        setEvaluationDateStrAndTimestamp();

        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        statsTableName = TableUtils.getFullTableName(statsTablePrefix, pipelineVersion);
        finishing();
        exportToS3AndAddToContext(statsTableName, ACCOUNT_STATS_TABLE_NAME);
    }

    private PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ProfileAccount");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        int step = 0;
        profileStep = step++;
        bucketStep = step;
        // -----------
        TransformationStepConfig profile = profile();
        TransformationStepConfig encode = bucketEncode();
        TransformationStepConfig calc = calcStats();

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(profile); //
        steps.add(encode); //
        steps.add(calc); //
        // -----------
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        addBaseTables(step, fullAccountTableName);
        step.setTransformer(TRANSFORMER_PROFILER);

        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        // Pass current timestamp as a configuration parameter to the profile step.
        conf.setEvaluationDateAsTimestamp(evaluationDateAsTimestamp);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucketEncode() {
        TransformationStepConfig step = new TransformationStepConfig();
        addBaseTables(step, fullAccountTableName);
        step.setInputSteps(Collections.singletonList(profileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);
        setTargetTable(step, statsTablePrefix);
        CalculateStatsConfig conf = new CalculateStatsConfig();
        step.setConfiguration(appendEngineConf(conf, extraHeavyEngineConfig()));
        return step;
    }

    private void enrichMasterTableSchema(String tableName) {
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        log.info("Attempt to enrich master table schema: " + table.getName());
        final List<Attribute> attrs = new ArrayList<>();
        final String evaluationDateStr = findEvaluationDate();
        final String ldrFieldValue = //
                StringUtils.isNotBlank(evaluationDateStr) ? ("Last Data Refresh: " + evaluationDateStr) : null;
        final AtomicLong updatedAttrs = new AtomicLong(0);
        table.getAttributes().forEach(attr0 -> {
            boolean updated = false;
            if (!attr0.hasTag(Tag.INTERNAL)) {
                attr0.setTags(Tag.INTERNAL);
                updated = true;
            }
            if (StringUtils.isNotBlank(ldrFieldValue) && LogicalDataType.Date.equals(attr0.getLogicalDataType())) {
                if (attr0.getLastDataRefresh() == null || !attr0.getLastDataRefresh().equals(ldrFieldValue)) {
                    log.info("Setting last data refresh for profile date attribute: " + attr0.getName() + " to "
                            + evaluationDateStr);
                    attr0.setLastDataRefresh(ldrFieldValue);
                    updated = true;
                }
            }
            if (updated) {
                updatedAttrs.incrementAndGet();
            }
            attrs.add(attr0);
        });
        if (updatedAttrs.get() > 0) {
            log.info("Found " + updatedAttrs.get() + " attrs to update, refresh master table schema.");
            table.setAttributes(attrs);
            String customerSpaceStr = customerSpace.toString();
            TableRoleInCollection batchStoreRole = BusinessEntity.Account.getBatchStore();
            String inactiveLink = dataCollectionProxy.getTableName(customerSpaceStr, batchStoreRole, inactive);
            String activeLink = dataCollectionProxy.getTableName(customerSpaceStr, batchStoreRole, active);
            metadataProxy.updateTable(customerSpaceStr, table.getName(), table);
            if (StringUtils.isNotBlank(inactiveLink) && inactiveLink.equalsIgnoreCase(table.getName())) {
                dataCollectionProxy.upsertTable(customerSpaceStr, inactiveLink, batchStoreRole, inactive);
            }
            if (StringUtils.isNotBlank(activeLink) && activeLink.equalsIgnoreCase(table.getName())) {
                dataCollectionProxy.upsertTable(customerSpaceStr, activeLink, batchStoreRole, active);
            }
        }
    }

    private void finishing() {
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
        enrichMasterTableSchema(masterTableName);
    }

}
