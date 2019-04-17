package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.EmptyFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceededCountCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.UnderPopulatedFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.LatticeCacheSeedChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(LatticeCacheSeedChkFlow.DATAFLOW_BEAN_NAME)
public class LatticeCacheSeedChkFlow extends ConfigurableFlowBase<LatticeCacheSeedChkConfig> {
    public final static String DATAFLOW_BEAN_NAME = "LatticeCacheSeedChkFlow";
    public final static String TRANSFORMER_NAME = "LatticeSeedFlowTransformer";

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
        return LatticeCacheSeedChkConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        LatticeCacheSeedChkConfig config = getTransformerConfig(parameters);
        Node latticeCacheSeed = addSource(parameters.getBaseTables().get(0));
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.add(latticeCacheSeed);
        ExceededCountCheckParam utilParams1 = new ExceededCountCheckParam();
        utilParams1.setExceedCountThreshold(config.getExceededCountThreshold());
        utilParams1.setCntLessThanThresholdFlag(config.getLessThanThresholdFlag());
        Node resultNode1 = CheckUtils.runCheck(nodeList, utilParams1);
        EmptyFieldCheckParam utilParams2 = new EmptyFieldCheckParam();
        utilParams2.setCheckEmptyField(config.getDomain());
        utilParams2.setIdentifierFields(config.getId());
        Node resultNode2 = CheckUtils.runCheck(nodeList, utilParams2);
        UnderPopulatedFieldCheckParam utilParams3 = new UnderPopulatedFieldCheckParam();
        List<String> fieldList = new ArrayList<String>();
        fieldList.add(config.getCheckField());
        utilParams3.setGroupByFields(fieldList);
        utilParams3.setThreshold(config.getThreshold());
        Node resultNode3 = CheckUtils.runCheck(nodeList, utilParams3);
        DuplicatedValueCheckParam utilParams4 = new DuplicatedValueCheckParam();
        utilParams4.setGroupByFields(config.getGroupByFields());
        utilParams4.setIdentifierFields(config.getId());
        Node resultNode4 = CheckUtils.runCheck(nodeList, utilParams4);
        Node finalResultNode = resultNode1 //
                .merge(resultNode2) //
                .merge(resultNode3) //
                .merge(resultNode4);
        return finalResultNode;
    }

}
