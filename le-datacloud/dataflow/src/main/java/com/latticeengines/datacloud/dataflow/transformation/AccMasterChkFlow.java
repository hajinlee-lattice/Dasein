package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.EmptyFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceededCountCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccMasterChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(AccMasterChkFlow.DATAFLOW_BEAN_NAME)
public class AccMasterChkFlow extends ConfigurableFlowBase<AccMasterChkConfig> {
    public final static String DATAFLOW_BEAN_NAME = "AccMasterChkFlow";
    public final static String TRANSFORMER_NAME = "AccMasterFlowTransformer";

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
        return AccMasterChkConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        AccMasterChkConfig config = getTransformerConfig(parameters);
        Node accMaster = addSource(parameters.getBaseTables().get(0));
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.add(accMaster);
        ExceededCountCheckParam exceedCntChkParams = new ExceededCountCheckParam();
        exceedCntChkParams.setExceedCountThreshold(config.getExceededCountThreshold());
        exceedCntChkParams.setCntLessThanThresholdFlag(config.getLessThanThresholdFlag());
        Node accMastrExceedCntNode = CheckUtils.runCheck(nodeList, exceedCntChkParams);
        DuplicatedValueCheckParam dupParamField1 = new DuplicatedValueCheckParam();
        dupParamField1.setGroupByFields(config.getGroupByFields());
        Node accMastrDupNodeField1 = CheckUtils.runCheck(nodeList, dupParamField1);
        DuplicatedValueCheckParam dupParamField2 = new DuplicatedValueCheckParam();
        List<String> grpByFields = new ArrayList<String>();
        grpByFields.add(config.getKey());
        dupParamField2.setGroupByFields(grpByFields);
        Node accMastrDupNodeField2 = CheckUtils.runCheck(nodeList, dupParamField2);
        EmptyFieldCheckParam emptyFieldChkParam = new EmptyFieldCheckParam();
        emptyFieldChkParam.setCheckEmptyField(config.getKey());
        emptyFieldChkParam.setIdentifierFields(config.getId());
        Node emptyFieldNode = CheckUtils.runCheck(nodeList, emptyFieldChkParam);
        Node resultNode = accMastrExceedCntNode //
                .merge(accMastrDupNodeField1) //
                .merge(accMastrDupNodeField2) //
                .merge(emptyFieldNode); //
        return resultNode;
    }

}
