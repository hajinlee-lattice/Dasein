package com.latticeengines.datacloud.dataflow.transformation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TestIterativeStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("testIterativeStepFlow")
public class TestIterativeStepFlow extends ConfigurableFlowBase<TestIterativeStepConfig> {
    private static final Log log = LogFactory.getLog(TestIterativeStepFlow.class);

    @Override
    public String getDataFlowBeanName() {
        return "testIterativeStepFlow";
    }

    @Override
    public String getTransformerName() {
        return "DummyIterativeTransformer";
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TestIterativeStepConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node dataSet1 = addSource(parameters.getBaseTables().get(0));
        int randomNumber = (int) (Math.random() * 100);
        log.info("Random number : " + randomNumber);
        if (randomNumber < 40) {
            Node dataSet2 = addSource(parameters.getBaseTables().get(1));
            dataSet1 = dataSet1.merge(dataSet2);
        }
        return dataSet1;
    }
}
