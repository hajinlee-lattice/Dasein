package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ContactNameConcatenateConfig;
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

    private int mergeStep;
    private int concatenateStep;
    private int entityMatchStep;
    private int upsertMasterStep;
    private int diffStep;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeContact");

            boolean entityMatchEnabled = configuration.isEntityMatchEnabled();

            int stepCount = 0;
            mergeStep = stepCount++;
            concatenateStep = stepCount++;
            if (entityMatchEnabled) {
                entityMatchStep = stepCount++;
            }
            upsertMasterStep = stepCount++;
            diffStep = stepCount;

            TransformationStepConfig merge = mergeInputs(false, true, false);
            TransformationStepConfig concatenate = concatenateContactName(mergeStep);
            TransformationStepConfig entityMatch = null;
            TransformationStepConfig upsertMaster;
            if (entityMatchEnabled) {
                // add bulk match between concat & merge master to perform lead to account match
                entityMatch = leadToAccountMatch(concatenateStep);
                upsertMaster = mergeMaster(entityMatchStep);
            } else {
                upsertMaster = mergeMaster(concatenateStep);
            }
            TransformationStepConfig diff = diff(concatenateStep, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(concatenate);
            if (entityMatchEnabled) {
                steps.add(entityMatch);
            }
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

    private TransformationStepConfig concatenateContactName(int mergeStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergeStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONTACT_NAME_CONCATENATER);
        ContactNameConcatenateConfig config = new ContactNameConcatenateConfig();
        config.setConcatenateFields(new String[] { InterfaceName.FirstName.name(), InterfaceName.LastName.name() });
        config.setResultField(InterfaceName.ContactName.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        if (!configuration.isEntityMatchEnabled()) {
            return;
        }

        table.getAttributes().forEach(attr -> {
            // update metadata for AccountId attribute since it is only created after lead
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

    /*
     * Lead to account match configurations
     */
    private TransformationStepConfig leadToAccountMatch(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer(TRANSFORMER_MATCH);
        String configStr = MatchUtils.getAllocateIdMatchConfigForContact(getBaseMatchInput(), getInputTableColumnNames(0));
        step.setConfiguration(configStr);
        return step;
    }
}
