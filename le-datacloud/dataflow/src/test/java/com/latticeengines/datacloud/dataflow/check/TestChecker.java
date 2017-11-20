package com.latticeengines.datacloud.dataflow.check;

import static com.latticeengines.datacloud.dataflow.check.TestChecker.DATAFLOW_BEAN;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConsolidateBaseFlow;
import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

@Component(DATAFLOW_BEAN)
public class TestChecker extends ConsolidateBaseFlow<TestCheckConfig> {

    public static final String TRANSFORMER_NAME = "testCheckTransformer";
    public static final String DATAFLOW_BEAN = "testCheckDataFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node input = addSource(parameters.getBaseTables().get(0));
        TestCheckConfig testCheckConfig = getTransformerConfig(parameters);
        return CheckUtils.runCheck(input, testCheckConfig.getCheckParam());
    }

    @Override
    public Class<? extends TestCheckConfig> getTransformerConfigClass() {
        return TestCheckConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

}
