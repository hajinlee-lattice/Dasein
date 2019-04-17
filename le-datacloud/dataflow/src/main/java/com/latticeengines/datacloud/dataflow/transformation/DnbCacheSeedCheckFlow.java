package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.check.EmptyFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceededCountCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DnbCacheSeedChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(DnbCacheSeedCheckFlow.DATAFLOW_BEAN_NAME)
public class DnbCacheSeedCheckFlow extends ConfigurableFlowBase<DnbCacheSeedChkConfig> {
    public final static String DATAFLOW_BEAN_NAME = "DnbCacheSeedCheckFlow";
    public final static String TRANSFORMER_NAME = "DnbSeedFlowTransformer";

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
        return DnbCacheSeedChkConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        DnbCacheSeedChkConfig config = getTransformerConfig(parameters);
        Node dnbCacheSeed = addSource(parameters.getBaseTables().get(0));
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.add(dnbCacheSeed);
        ExceededCountCheckParam exceedCntChkParams = new ExceededCountCheckParam();
        exceedCntChkParams.setExceedCountThreshold(config.getExceededCountThreshold());
        exceedCntChkParams.setCntLessThanThresholdFlag(config.getLessThanThresholdFlag());
        Node exceedCntNode = CheckUtils.runCheck(nodeList, exceedCntChkParams);
        EmptyFieldCheckParam emptyFieldChkParam = new EmptyFieldCheckParam();
        emptyFieldChkParam.setCheckEmptyField(config.getDuns());
        emptyFieldChkParam.setIdentifierFields(config.getId());
        Node emptyFieldNode = CheckUtils.runCheck(nodeList, emptyFieldChkParam);
        Node resultNode = exceedCntNode //
                .merge(emptyFieldNode);
        return resultNode;
    }

}
