package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceededCountCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccMastrLookupChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(AccMasterLookupChkFlow.DATAFLOW_BEAN_NAME)
public class AccMasterLookupChkFlow extends ConfigurableFlowBase<AccMastrLookupChkConfig> {
    public final static String DATAFLOW_BEAN_NAME = "AccMasterLookupChkFlow";
    public final static String TRANSFORMER_NAME = "AMLookupFlowTransformer";

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
        return AccMastrLookupChkConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        AccMastrLookupChkConfig config = getTransformerConfig(parameters);
        Node accMasterLookup = addSource(parameters.getBaseTables().get(0));
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.add(accMasterLookup);
        ExceededCountCheckParam exceedCntChkParams = new ExceededCountCheckParam();
        exceedCntChkParams.setExceedCountThreshold(config.getExceededCountThreshold());
        exceedCntChkParams.setCntLessThanThresholdFlag(config.getLessThanThresholdFlag());
        Node accMastrLookupExceedCntNode = CheckUtils.runCheck(nodeList, exceedCntChkParams);
        DuplicatedValueCheckParam dupParam = new DuplicatedValueCheckParam();
        dupParam.setGroupByFields(config.getGroupByFields());
        dupParam.setIdentifierFields(config.getId());
        Node accMastrLookupDupNode = CheckUtils.runCheck(nodeList, dupParam);
        Node resultNode = accMastrLookupExceedCntNode //
                .merge(accMastrLookupDupNode);
        return resultNode;
    }

}
