package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPY_TXMFR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.datacloud.match.RefreshFrequency;
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
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component(ProfileAccount.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileAccount extends ProfileStepBase<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "profileAccount";

    private static final Logger log = LoggerFactory.getLogger(ProfileAccount.class);
    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    private int filterStep;
    private int profileStep;
    private int bucketStep;

    private String fullAccountTableName;
    private String masterTableName;
    private String statsTableName;
    private String statsTablePrefix = "Stats";

    private DataCollection.Version active;
    private DataCollection.Version inactive;

    private boolean ldcRefresh;
    private boolean hasFilter;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Account;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        masterTableName = ensureInactiveBatchStoreExists();
        if (StringUtils.isBlank(masterTableName)) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }

        boolean shortCut;
        Table statsTableInCtx = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_STATS_TABLE_NAME);
        shortCut = statsTableInCtx != null;

        if (shortCut) {
            log.info("Found stats table in context, going thru short-cut mode.");
            statsTableName = statsTableInCtx.getName();
            finishing();
            return null;
        } else {
            // reset result table names
            statsTableName = null;

            fullAccountTableName = getStringValueFromContext(FULL_ACCOUNT_TABLE_NAME);
            if (StringUtils.isBlank(fullAccountTableName)) {
                throw new IllegalStateException("Cannot find the fully enriched account table");
            }
            Table fullAccountTable = metadataProxy.getTableSummary(customerSpace.toString(), fullAccountTableName);
            if (fullAccountTable == null) {
                throw new IllegalStateException("Cannot find the fully enriched account table in default collection");
            }
            double sizeInGb = ScalingUtils.getTableSizeInGb(yarnConfiguration, fullAccountTable);
            scalingMultiplier = ScalingUtils.getMultiplier(sizeInGb);
            log.info("Set scalingMultiplier=" + scalingMultiplier + " base on master table size=" + sizeInGb + " gb.");

            setEvaluationDateStrAndTimestamp();
            checkDataChanges();

            PipelineTransformationRequest request = getTransformRequest();
            return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
        }
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
        if (hasFilter) {
            filterStep = step++;
        }
        profileStep = step++;
        bucketStep = step;
        // -----------
        TransformationStepConfig filter = hasFilter ? filter() : null;
        TransformationStepConfig profile = profile(hasFilter);
        TransformationStepConfig encode = bucketEncode(hasFilter);
        TransformationStepConfig calc = calcStats();

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        if (hasFilter) {
            steps.add(filter); //
        }
        steps.add(profile); //
        steps.add(encode); //
        steps.add(calc); //
        // -----------
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig filter() {
        TransformationStepConfig step = new TransformationStepConfig();
        addBaseTables(step, fullAccountTableName);
        step.setTransformer(TRANSFORMER_COPY_TXMFR);
        CopyConfig conf = new CopyConfig();
        conf.setSelectAttrs(getRetrainAttrNames());
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private List<String> getRetrainAttrNames() {
        List<String> retainAttrNames = null;
        List<String> fullAccountTableColumns = metadataProxy
                .getTableColumns(getConfiguration().getCustomerSpace().toString(), fullAccountTableName).stream()
                .map(c -> c.getAttrName()).collect(Collectors.toList());
        if (ldcRefresh) {
            Set<String> releaseColumnNames = columnMetadataProxy.getAllColumns(getConfiguration().getDataCloudVersion())
                    .stream().filter(column -> column.getRefreshFrequency() == RefreshFrequency.RELEASE)
                    .map(column -> column.getAttrName()).collect(Collectors.toSet());
            retainAttrNames = fullAccountTableColumns.stream().filter(c -> !releaseColumnNames.contains(c))
                    .collect(Collectors.toList());
        } else {
            Set<String> allColumnNames = columnMetadataProxy.getAllColumns(getConfiguration().getDataCloudVersion())
                    .stream().map(column -> column.getAttrName()).collect(Collectors.toSet());
            retainAttrNames = fullAccountTableColumns.stream().filter(c -> !allColumnNames.contains(c))
                    .collect(Collectors.toList());
        }
        return retainAttrNames;
    }

    private TransformationStepConfig profile(boolean hasFilter) {
        TransformationStepConfig step = new TransformationStepConfig();
        if (hasFilter) {
            step.setInputSteps(Collections.singletonList(filterStep));
        } else {
            addBaseTables(step, fullAccountTableName);
        }
        step.setTransformer(TRANSFORMER_PROFILER);

        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        // Pass current timestamp as a configuration parameter to the profile
        // step.
        conf.setEvaluationDateAsTimestamp(evaluationDateAsTimestamp);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucketEncode(boolean hasFilter) {
        TransformationStepConfig step = new TransformationStepConfig();
        if (hasFilter) {
            step.setInputSteps(Arrays.asList(filterStep, profileStep));
        } else {
            addBaseTables(step, fullAccountTableName);
            step.setInputSteps(Collections.singletonList(profileStep));
        }
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

    private void checkDataChanges() {
        if (checkManyUpdate()) {
            log.info("There's many new or updated records, compute stats for all columns.");
            return;
        }
        ChoreographerContext grapherContext = getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        boolean ldcChange = grapherContext != null && grapherContext.isDataCloudChanged();
        ldcRefresh = grapherContext != null && grapherContext.isDataCloudRefresh();
        hasFilter = (!ldcChange || ldcRefresh) && !grapherContext.isDataCloudNew();
        boolean enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
        hasFilter = hasFilter && !enforceRebuild;
        putStringValueInContext(PROCESS_ACCOUNT_STATS_MERGE, hasFilter + "");
        log.info("hasFilter=" + hasFilter + " ldcChange=" + ldcChange + " ldcRefresh=" + ldcRefresh + " ldcNew="
                + grapherContext.isDataCloudNew() + " enforceRebuild=" + enforceRebuild);
    }

    private boolean checkManyUpdate() {
        Long existingCount = null;
        Long updateCount = null;
        Long newCount = null;
        Map<BusinessEntity, Long> existingValueMap = getMapObjectFromContext(BaseWorkflowStep.EXISTING_RECORDS,
                BusinessEntity.class, Long.class);
        if (existingValueMap != null) {
            existingCount = existingValueMap.get(BusinessEntity.Account);
        }
        Map<BusinessEntity, Long> newValueMap = getMapObjectFromContext(BaseWorkflowStep.NEW_RECORDS,
                BusinessEntity.class, Long.class);
        if (newValueMap != null) {
            newCount = newValueMap.get(BusinessEntity.Account);
        }
        Map<BusinessEntity, Long> updateValueMap = getMapObjectFromContext(BaseWorkflowStep.UPDATED_RECORDS,
                BusinessEntity.class, Long.class);
        if (updateValueMap != null) {
            updateCount = updateValueMap.get(BusinessEntity.Account);
        }
        long diffCount = (newCount == null ? 0L : newCount) + (updateCount == null ? 0L : updateCount);
        if (existingCount != null && existingCount != 0L) {
            float diffRate = diffCount * 1.0F / existingCount;
            return diffRate >= 0.3;
        }
        return false;
    }

    private void finishing() {
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
        enrichMasterTableSchema(masterTableName);
    }

}
