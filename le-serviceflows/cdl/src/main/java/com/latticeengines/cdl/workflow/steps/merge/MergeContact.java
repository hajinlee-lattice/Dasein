package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
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

    private int upsertMasterStep;
    private int diffStep;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeContact");
            String matchedTable = getMatchedTable();

            upsertMasterStep = 0;
            diffStep = 1;
            TransformationStepConfig upsertMaster = mergeMaster(configuration.isEntityMatchEnabled(), matchedTable);
            TransformationStepConfig diff = diff(matchedTable, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);
            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(upsertMaster);
            steps.add(diff);
            steps.add(report);
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private String getMatchedTable() {
        String matchedTable = getStringValueFromContext(ENTITY_MATCH_CONTACT_TARGETTABLE);
        if (StringUtils.isBlank(matchedTable)) {
            throw new RuntimeException("There's no matched table found!");
        }
        return matchedTable;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        if (!configuration.isEntityMatchEnabled()) {
            return;
        }
        table.getAttributes().forEach(attr -> {
            // update metadata for AccountId attribute since it is only created
            // after lead
            // to account match and does not have the correct metadata
            if (InterfaceName.AccountId.name().equals(attr.getName())) {
                attr.setInterfaceName(InterfaceName.AccountId);
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
        });
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }
}
