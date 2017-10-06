package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateContactDataStepConfiguration;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;

@Component("consolidateContactData")
public class ConsolidateContactData extends ConsolidateDataBase<ConsolidateContactDataStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateContactData.class);

    private int mergeStep, upsertMasterStep, diffStep, bucketDiffStep;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        srcIdField = configuration.getIdField();
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");

            mergeStep = 0;
            upsertMasterStep = 1;
            diffStep = 2;
            bucketDiffStep = 3;
            TransformationStepConfig merge = mergeInputs(false);
            TransformationStepConfig upsertMaster = mergeMaster(mergeStep);
            TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
            TransformationStepConfig bucketDiff = bucket(diffStep, false);
            TransformationStepConfig sort = sortDiff(bucketDiffStep, 50);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(upsertMaster);
            if (isBucketing()) {
                steps.add(diff);
                steps.add(bucketDiff);
                steps.add(sort);
            }
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void findProfileTable() {
        Table profileTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ContactProfile);
        if (profileTable != null) {
            profileTableName = profileTable.getName();
            log.info("Set profileTableName=" + profileTableName);
        } else {
            log.info("There's no profileTableName");
        }
    }

    @Override
    protected void setupConfig(ConsolidateDataTransformerConfig config) {
        config.setMasterIdField(TableRoleInCollection.ConsolidatedContact.getPrimaryKey().name());
    }

}
