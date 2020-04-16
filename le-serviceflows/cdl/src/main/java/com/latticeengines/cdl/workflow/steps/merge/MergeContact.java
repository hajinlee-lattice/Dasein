package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
        if (StringUtils.isBlank(matchedContactTable)) { // && skipSoftDelete) {
            if (configuration.isEntityMatchEnabled()) {
                log.warn("There's no matched table found, and no soft delete action!");
            } else {
                throw new RuntimeException("There's no matched table found, and no soft delete action!");
            }
        }

        double oldTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, masterTable);
        if (StringUtils.isBlank(matchedContactTable)) {
            scalingMultiplier = ScalingUtils.getMultiplier(oldTableSize + oldTableSize);
            log.info(String.format("Adjust scalingMultiplier=%d based on the size of two tables %.2f g.", //
                    scalingMultiplier, oldTableSize + oldTableSize));
        } else {
            Table tableSummary = metadataProxy.getTableSummary(customerSpace.toString(), matchedContactTable);
            double newTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, tableSummary);
            newTableSize += sumTableSizeFromMapCtx(ENTITY_MATCH_STREAM_CONTACT_TARGETTABLE);
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
        int changeListStep;
        TransformationStepConfig upsert;
        TransformationStepConfig diff;
        TransformationStepConfig changeList;
        TransformationStepConfig reportChangeList;
        TransformationStepConfig report;
        if (configuration.isEntityMatchEnabled()) {
            List<TransformationStepConfig> extracts = new ArrayList<>();
            List<Integer> extractSteps = new ArrayList<>();
            addNewContactExtractStepsForActivityStream(extracts, extractSteps);
            steps.addAll(extracts);
            boolean noImports = CollectionUtils.isEmpty(extractSteps) && StringUtils.isEmpty(matchedTable);
            if (noImports) {
                if (!hasSystemBatch) {
                    throw new IllegalArgumentException("There's no merge or soft delete!");
                } else {
                    log.warn("There's no import!");
                }
            } else {
                TransformationStepConfig dedup = dedupAndMerge(InterfaceName.ContactId.name(), //
                        CollectionUtils.isEmpty(extractSteps) ? null : extractSteps, //
                        StringUtils.isBlank(matchedTable) ? null : Collections.singletonList(matchedTable), //
                        Arrays.asList(InterfaceName.CustomerAccountId.name(), InterfaceName.CustomerContactId.name()));
                steps.add(dedup);
            }
            int dedupStep = extractSteps.size();
            upsertMasterStep = dedupStep + 1;
            TransformationStepConfig upsertSystem = null;
            if (hasSystemBatch) {
                if (noImports) {
                    upsertSystem = upsertSystemBatch(-1, true);
                    upsert = mergeSystemBatch(0, true);
                    steps.add(upsertSystem);
                    steps.add(upsert);
                    request.setSteps(steps);
                    return request;
                }
                upsertSystem = upsertSystemBatch(dedupStep, true);
                upsert = mergeSystemBatch(upsertMasterStep, true);
                upsertMasterStep++;
            } else {
                upsert = upsertMaster(true, dedupStep, true);
            }
            diffStep = upsertMasterStep + 1;
            changeListStep = upsertMasterStep + 2;
            diff = diff(matchedTable, upsertMasterStep);
            if (upsertSystem != null) {
                steps.add(upsertSystem);
            }
            steps.add(upsert);
        } else {
            upsertMasterStep = 0;
            diffStep = 1;
            changeListStep = 2;
            upsert = upsertMaster(false, matchedTable);
            diff = diff(matchedTable, upsertMasterStep);
            steps.add(upsert);
        }
        String joinKey = configuration.isEntityMatchEnabled() ? InterfaceName.EntityId.name()
                : InterfaceName.AccountId.name();
        changeList = createChangeList(upsertMasterStep, joinKey);
        reportChangeList = reportChangeList(changeListStep);
        report = reportDiff(diffStep);
        steps.add(diff);
        steps.add(changeList);
        steps.add(reportChangeList);
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
        exportToDynamo(batchStoreTableName, TableRoleInCollection.ConsolidatedContact.getPartitionKey(),
                TableRoleInCollection.ConsolidatedContact.getRangeKey());
    }

    private void addNewContactExtractStepsForActivityStream(@NotNull List<TransformationStepConfig> extracts,
            @NotNull List<Integer> extractSteps) {
        // contact + account system ids
        List<String> systemIds = new ArrayList<>();
        systemIds.add(InterfaceName.CustomerAccountId.name());
        systemIds.add(InterfaceName.CustomerContactId.name());
        systemIds.addAll(getSystemIds(BusinessEntity.Account));
        systemIds.addAll(getSystemIds(BusinessEntity.Contact));

        log.info("SystemIds for embedded contact from stream = {}", systemIds);

        BiFunction<String, String, TransformationStepConfig> factory = getExtractNewEntitiesStepFactory(
                BusinessEntity.Contact, InterfaceName.ContactId, systemIds);

        addEmbeddedEntitiesFromActivityStream(extracts, extractSteps, BusinessEntity.Contact,
                ENTITY_MATCH_STREAM_CONTACT_TARGETTABLE, factory);
    }
}
