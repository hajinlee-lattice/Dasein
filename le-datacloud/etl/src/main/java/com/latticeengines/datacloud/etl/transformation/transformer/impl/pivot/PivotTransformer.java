package com.latticeengines.datacloud.etl.transformation.transformer.impl.pivot;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.pivot.PivotBuiltWith;
import com.latticeengines.datacloud.dataflow.transformation.pivot.PivotHGData;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractDataflowTransformer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PivotTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(PivotTransformer.TRANSFORMER_NAME)
public class PivotTransformer extends AbstractDataflowTransformer<PivotTransformerConfig, TransformationFlowParameters> {

    private static final Logger log = LoggerFactory.getLogger(PivotTransformer.class);
    public static final String TRANSFORMER_NAME = "pivotTransformer";

    private String dataFlowBeanName;

    @Inject
    SourceColumnEntityMgr sourceColumnEntityMgr;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return PivotTransformerConfig.class;
    }

    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, TransformationFlowParameters parameters,
                                         PivotTransformerConfig configuration) {
        dataFlowBeanName = configuration.getBeanName();

        //obsolete, this is done automatically by AbstractDataflowTransformer
        //setupParameters(parameters);

    }

    //obsolete now.
    private void setupParameters(TransformationFlowParameters parameters) {

        //time stamp field name in target src
        parameters.setTimestampField("Timestamp");

        String targetSrc = null;
        switch (dataFlowBeanName) {
            case PivotBuiltWith.BEAN_NAME:
                targetSrc = "BuiltWithPivoted";
                break;
            case PivotHGData.BEAN_NAME:
                targetSrc = "HGDataPivoted";
                break;
            default:
                log.error("invalid pivot flow bean name: " + dataFlowBeanName);
                break;
        }
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(targetSrc));

    }
}
