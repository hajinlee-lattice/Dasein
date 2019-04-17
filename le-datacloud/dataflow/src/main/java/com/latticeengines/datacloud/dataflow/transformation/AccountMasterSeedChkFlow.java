package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.EmptyFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceededCountCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.IncompleteCoverageForColChkParam;
import com.latticeengines.domain.exposed.datacloud.check.OutOfCoverageForRowChkParam;
import com.latticeengines.domain.exposed.datacloud.check.UnderPopulatedFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccountMasterSeedChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(AccountMasterSeedChkFlow.DATAFLOW_BEAN_NAME)
public class AccountMasterSeedChkFlow extends ConfigurableFlowBase<AccountMasterSeedChkConfig> {
    public final static String DATAFLOW_BEAN_NAME = "AccountMasterSeedChkFlow";
    public final static String TRANSFORMER_NAME = "AMSeedFlowTransformer";

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
        return AccountMasterSeedChkConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        AccountMasterSeedChkConfig config = getTransformerConfig(parameters);
        Node accountMasterSeed = addSource(parameters.getBaseTables().get(0));
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.add(accountMasterSeed);
        ExceededCountCheckParam exceedCntParams = new ExceededCountCheckParam();
        exceedCntParams.setExceedCountThreshold(config.getExceededCountThreshold());
        exceedCntParams.setCntLessThanThresholdFlag(config.getLessThanThresholdFlag());
        Node exceedCntNode = CheckUtils.runCheck(nodeList, exceedCntParams);
        DuplicatedValueCheckParam dupValChkParams = new DuplicatedValueCheckParam();
        dupValChkParams.setGroupByFields(config.getGroupByFields());
        Node dupValNode = CheckUtils.runCheck(nodeList, dupValChkParams);
        EmptyFieldCheckParam emptyFieldChkParam = new EmptyFieldCheckParam();
        emptyFieldChkParam.setCheckEmptyField(config.getDomain());
        emptyFieldChkParam.setIdentifierFields(config.getId());
        Node emptyFieldNode = CheckUtils.runCheck(nodeList, emptyFieldChkParam);
        UnderPopulatedFieldCheckParam underPopChk = new UnderPopulatedFieldCheckParam();
        List<String> fieldList = new ArrayList<String>();
        fieldList.add(config.getCheckField());
        underPopChk.setGroupByFields(fieldList);
        underPopChk.setThreshold(config.getThreshold());
        Node underPopNode = CheckUtils.runCheck(nodeList, underPopChk);
        IncompleteCoverageForColChkParam incompCoverCol = new IncompleteCoverageForColChkParam();
        List<String> grpFieldList = new ArrayList<String>();
        grpFieldList.add(config.getDomainSource());
        incompCoverCol.setGroupByFields(grpFieldList);
        incompCoverCol.setExpectedFieldValues(config.getExpectedCoverageFields());
        Node incompCovColNode = CheckUtils.runCheck(nodeList, incompCoverCol);
        OutOfCoverageForRowChkParam incompCoverRow = new OutOfCoverageForRowChkParam();
        incompCoverRow.setGroupByFields(grpFieldList);
        incompCoverRow.setExpectedFieldValues(config.getExpectedCoverageFields());
        String keyField = "";
        for (int i = 0; i < config.getId().size(); i++) {
            if (i == 0) {
                keyField += config.getId().get(i);
            } else {
                keyField += "," + config.getId().get(i);
            }
        }
        incompCoverRow.setKeyField(keyField);
        Node incompCovRowNode = CheckUtils.runCheck(nodeList, incompCoverRow);
        DuplicatedValueCheckParam dupValChkField = new DuplicatedValueCheckParam();
        List<String> grpByFields = new ArrayList<String>();
        grpByFields.add(config.getKey());
        dupValChkField.setGroupByFields(grpByFields);
        Node dupFieldNode = CheckUtils.runCheck(nodeList, dupValChkField);
        Node finalResultNode = exceedCntNode //
                .merge(dupValNode) //
                .merge(emptyFieldNode) //
                .merge(underPopNode) //
                .merge(incompCovColNode) //
                .merge(incompCovRowNode) //
                .merge(dupFieldNode);
        return finalResultNode;
    }

}
