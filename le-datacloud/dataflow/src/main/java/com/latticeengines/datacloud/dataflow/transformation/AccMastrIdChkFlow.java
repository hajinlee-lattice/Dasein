package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccMastrIdChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(AccMastrIdChkFlow.DATAFLOW_BEAN_NAME)
public class AccMastrIdChkFlow extends ConfigurableFlowBase<AccMastrIdChkConfig> {
    public final static String DATAFLOW_BEAN_NAME = "AccMastrIdChkFlow";
    public final static String TRANSFORMER_NAME = "AccMastrIdChkTransformer";
    public final static String ACTIVE = "ACTIVE";

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
        return AccMastrIdChkConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        AccMastrIdChkConfig config = getTransformerConfig(parameters);
        Node accMastrId = addSource(parameters.getBaseTables().get(0));
        List<Node> allRecords = new ArrayList<Node>();
        allRecords.add(accMastrId);
        String filterStatusExp = config.getStatus() + ".equals(\"" + ACTIVE + "\")";
        Node filteredStatus = accMastrId //
                .filter(filterStatusExp, new FieldList(accMastrId.getFieldNames()));
        List<Node> filteredNode = new ArrayList<Node>();
        filteredNode.add(filteredStatus);
        DuplicatedValueCheckParam dupValChkLatId = new DuplicatedValueCheckParam();
        dupValChkLatId.setGroupByFields(config.getGroupByFields());
        dupValChkLatId.setCheckDupWithStatus(config.getCheckDupWithStatus());
        Node dupLatId = CheckUtils.runCheck(filteredNode, dupValChkLatId);
        DuplicatedValueCheckParam dupValChkRedId = new DuplicatedValueCheckParam();
        List<String> grpByRedirId = new ArrayList<String>();
        grpByRedirId.add(config.getRedirectFromId());
        dupValChkRedId.setGroupByFields(grpByRedirId);
        Node dupRedId = CheckUtils.runCheck(allRecords, dupValChkRedId);
        DuplicatedValueCheckParam dupValChkDomDuns = new DuplicatedValueCheckParam();
        List<String> grpByDomDuns = new ArrayList<String>();
        grpByDomDuns.add(config.getDomain());
        grpByDomDuns.add(config.getDuns());
        dupValChkDomDuns.setGroupByFields(grpByDomDuns);
        Node dupDomDuns = CheckUtils.runCheck(allRecords, dupValChkDomDuns);
        Node resultNode = dupLatId //
                .merge(dupRedId) //
                .merge(dupDomDuns);
        return resultNode;
    }
}
