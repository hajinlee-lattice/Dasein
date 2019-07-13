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

@Component(MergeContact.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeContact extends BaseSingleEntityMergeImports<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeContact.class);

    static final String BEAN_NAME = "mergeContact";

    private String matchedContactTable;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        try {
            matchedContactTable = getStringValueFromContext(ENTITY_MATCH_CONTACT_TARGETTABLE);
            if (StringUtils.isBlank(matchedContactTable)) {
                throw new RuntimeException("There's no matched table found!");
            }

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeContact");
            String matchedTable = matchedContactTable;
            List<TransformationStepConfig> steps = new ArrayList<>();
            int upsertMasterStep;
            int diffStep;
            TransformationStepConfig upsert;
            TransformationStepConfig diff;
            if (configuration.isEntityMatchEnabled()) {
                int dedupStep = 0;
                upsertMasterStep = 1;
                diffStep = 2;
                TransformationStepConfig dedup = dedupAndMerge(InterfaceName.ContactId.name(), null,
                        Collections.singletonList(matchedTable));
                upsert = upsertMaster(true, dedupStep);
                diff = diff(dedupStep, upsertMasterStep);
                steps.add(dedup);
            } else {
                upsertMasterStep = 0;
                diffStep = 1;
                upsert = upsertMaster(false, matchedTable);
                diff = diff(matchedTable, upsertMasterStep);
            }
            steps.add(upsert);
            steps.add(diff);
            TransformationStepConfig report = reportDiff(diffStep);
            steps.add(report);
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
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
