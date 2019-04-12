package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPY_TXMFR;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class BaseProcessDiffStep<T extends BaseProcessEntityStepConfiguration>
        extends BaseTransformWrapperStep<T> {

    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected BusinessEntity entity;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
    }

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    protected abstract PipelineTransformationRequest getTransformRequest();

    protected TransformationStepConfig retainFields(int previousStep, TableRoleInCollection role) {
        return retainFields(previousStep, null, null, false, role);
    }

    protected TransformationStepConfig retainFields(int previousStep, String tgtTablePrefix, String primaryKey,
                                                    TableRoleInCollection role) {
        return retainFields(previousStep, tgtTablePrefix, primaryKey, true, role);
    }

    protected TransformationStepConfig retainFields(int previousStep, String tgtTablePrefix, String primaryKey,
                                                    boolean useTargetTable, TableRoleInCollection role) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(previousStep));
        step.setTransformer(TRANSFORMER_COPY_TXMFR);

        if (useTargetTable) {
            setTargetTable(step, tgtTablePrefix, primaryKey);
        }

        CopyConfig config = new CopyConfig();
        Table servingTable = dataCollectionProxy.getTable(customerSpace.toString(), role);
        if (servingTable != null) {
            List<String> fieldsToRetain = AvroUtils.getSchemaFields(yarnConfiguration,
                    servingTable.getExtracts().get(0).getPath());
            config.setSelectAttrs(fieldsToRetain);
        }
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

}
