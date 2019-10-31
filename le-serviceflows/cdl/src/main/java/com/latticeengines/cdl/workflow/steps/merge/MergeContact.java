package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(MergeContact.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeContact extends BaseSingleEntityMergeImports<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeContact.class);

    static final String BEAN_NAME = "mergeContact";

    private String matchedContactTable;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        matchedContactTable = getStringValueFromContext(ENTITY_MATCH_CONTACT_TARGETTABLE);
        if (StringUtils.isBlank(matchedContactTable) && skipSoftDelete) {
            throw new RuntimeException("There's no matched table found, and no soft delete action!");
        }

        double oldTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, masterTable);
        if (StringUtils.isBlank(matchedContactTable)) {
            scalingMultiplier = ScalingUtils.getMultiplier(oldTableSize + oldTableSize);
            log.info(String.format("Adjust scalingMultiplier=%d based on the size of two tables %.2f g.", //
                    scalingMultiplier, oldTableSize + oldTableSize));
        } else {
            Table tableSummary = metadataProxy.getTableSummary(customerSpace.toString(), matchedContactTable);
            double newTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, tableSummary);
            scalingMultiplier = ScalingUtils.getMultiplier(oldTableSize + newTableSize);
            log.info(String.format("Adjust scalingMultiplier=%d based on the size of two tables %.2f g.", //
                    scalingMultiplier, oldTableSize + newTableSize));
        }
    }

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MergeContact");
        String matchedTable = matchedContactTable;
        List<TransformationStepConfig> steps = new ArrayList<>();
        int upsertMasterStep;
        int diffStep;
        TransformationStepConfig upsert;
        TransformationStepConfig mergeSoftDelete = null;
        TransformationStepConfig softDelete = null;
        TransformationStepConfig diff;
        if (configuration.isEntityMatchEnabled()) {
            boolean noImports = StringUtils.isEmpty(matchedTable);
            if (noImports) {
                int softDeleteMergeStep = 0;
                int softDeleteStep = softDeleteMergeStep + 1;
                diffStep = softDeleteStep + 1;
                if (skipSoftDelete) {
                    throw new IllegalArgumentException("There's no merge or soft delete!");
                } else {
                    mergeSoftDelete = mergeSoftDelete(softDeleteActions);
                    softDelete = softDelete(softDeleteMergeStep, inputMasterTableName);
                    diff = diff(inputMasterTableName, softDeleteStep);
                }
            } else {
                int dedupStep = 0;
                upsertMasterStep = 1;
                int softDeleteMergeStep = upsertMasterStep + 1;
                int softDeleteStep = softDeleteMergeStep + 1;
                diffStep = softDeleteStep + 1;
                TransformationStepConfig dedup = dedupAndMerge(InterfaceName.ContactId.name(), null,
                        Collections.singletonList(matchedTable), //
                        Arrays.asList(InterfaceName.CustomerAccountId.name(), InterfaceName.CustomerContactId.name()));
                upsert = upsertMaster(true, dedupStep, skipSoftDelete);
                if (!skipSoftDelete) {
                    mergeSoftDelete = mergeSoftDelete(softDeleteActions);
                    softDelete = softDelete(softDeleteMergeStep, upsertMasterStep);
                }
                diff = diff(dedupStep, softDeleteStep);
                steps.add(dedup);
                steps.add(upsert);
            }
        } else {
            upsertMasterStep = 0;
            diffStep = 1;
            upsert = upsertMaster(false, matchedTable);
            diff = diff(matchedTable, upsertMasterStep);
            steps.add(upsert);
        }

        if (configuration.isEntityMatchEnabled() && !skipSoftDelete) {
            steps.add(mergeSoftDelete);
            steps.add(softDelete);
        }
        steps.add(diff);
        TransformationStepConfig report = reportDiff(diffStep);
        steps.add(report);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        Map<String, Attribute> attrsToInherit = new HashMap<>();
        addAttrsToMap(attrsToInherit, inputMasterTableName);
        addAttrsToMap(attrsToInherit, matchedContactTable);
        updateAttrs(table, attrsToInherit);
        if (configuration.isEntityMatchEnabled()) {
            table.getAttributes().forEach(attr -> {
                // update metadata for AccountId attribute since it is only created
                // after lead to account match and does not have the correct metadata
                if (InterfaceName.AccountId.name().equals(attr.getName())) {
                    attr.setInterfaceName(InterfaceName.AccountId);
                    attr.setDisplayName("Atlas Account ID");
                    attr.setTags(Tag.INTERNAL);
                    attr.setLogicalDataType(LogicalDataType.Id);
                    attr.setNullable(false);
                    attr.setApprovedUsage(ApprovedUsage.NONE);
                    attr.setSourceLogicalDataType(attr.getPhysicalDataType());
                    attr.setCategory(Category.CONTACT_ATTRIBUTES.name());
                    attr.setAllowedDisplayNames(
                            Arrays.asList("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT"));
                    attr.setFundamentalType(FundamentalType.ALPHA.getName());
                }
                if (InterfaceName.ContactId.name().equals(attr.getName())) {
                    attr.setDisplayName("Atlas Contact ID");
                }
            });
        }
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        String batchStoreTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        checkAttributeLimit(batchStoreTableName, configuration.isEntityMatchEnabled());
    }
}
