package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ManSeedChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(ManSeedChkFlow.DATAFLOW_BEAN_NAME)
public class ManSeedChkFlow extends ConfigurableFlowBase<ManSeedChkConfig> {
    public final static String DATAFLOW_BEAN_NAME = "ManSeedChkFlow";
    public final static String TRANSFORMER_NAME = "ManSeedChkTransformer";

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ManSeedChkConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        ManSeedChkConfig config = getTransformerConfig(parameters);
        Node manSeed = addSource(parameters.getBaseTables().get(0));
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.add(manSeed);
        DuplicatedValueCheckParam dupParam = new DuplicatedValueCheckParam();
        dupParam.setGroupByFields(config.getGroupByFields());
        dupParam.setIdentifierFields(config.getId());
        Node manSeedDupNode = CheckUtils.runCheck(nodeList, dupParam);
        return manSeedDupNode;
    }

}
