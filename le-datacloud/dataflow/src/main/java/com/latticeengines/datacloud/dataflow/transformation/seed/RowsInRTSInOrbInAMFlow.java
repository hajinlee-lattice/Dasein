package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.RowsInRTSInOrbInAMConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(RowsInRTSInOrbInAMFlow.DATAFLOW_BEAN_NAME)
public class RowsInRTSInOrbInAMFlow extends ConfigurableFlowBase<RowsInRTSInOrbInAMConfig> {

    public static final String DATAFLOW_BEAN_NAME = "RowsInRTSInOrbInAMFlow";
    public static final String TRANSFORMER_NAME = "RowsInRTSInOrbInAMTransformer";
    public static final String RTS = "RTS";
    public static final String DOMAIN_SRC_RTS = "DOMAIN_SRC_RTS";
    public static final String RENAME = "RENAMED_";

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
        return RowsInRTSInOrbInAMConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        RowsInRTSInOrbInAMConfig config = getTransformerConfig(parameters);
        Node accountMasterSeed = addSource(parameters.getBaseTables().get(0));
        Node orbCacheSeedSecDom = addSource(parameters.getBaseTables().get(1));
        Node orbCacheSeedStandrd = addSource(parameters.getBaseTables().get(2));
        String filterDomainSrc = config.getDomainSource() + ".equals(\"" + RTS + "\")";
        String checkNullExpression = config.getDomain() + " != null  && " + "!" + config.getDomain() + ".equals(\"\")";
        Node filteredDomSource = accountMasterSeed //
                .filter(filterDomainSrc, new FieldList(config.getDomainSource())); //
        List<String> fieldNames = filteredDomSource.getFieldNames();
        fieldNames.remove(config.getDomain());
        fieldNames.add(RENAME + config.getDomain());
        Node filterNotNullDom = filteredDomSource //
                .filter(checkNullExpression, new FieldList(config.getDomain())) //
                .rename(new FieldList(config.getDomain()), new FieldList(RENAME + config.getDomain())) //
                .retain(new FieldList(fieldNames));
        Node rowsExistInOrbSecDom = filterNotNullDom //
                .join(RENAME + config.getDomain(), orbCacheSeedSecDom, config.getSecDomain(), JoinType.INNER) //
                .retain(new FieldList(filterNotNullDom.getFieldNames()));
        Node rowsExistInOrbStand = filterNotNullDom //
                .join(RENAME + config.getDomain(), orbCacheSeedStandrd, config.getDomain(), JoinType.INNER) //
                .retain(new FieldList(rowsExistInOrbSecDom.getFieldNames()));
        Node mergedResult = rowsExistInOrbSecDom //
                .merge(rowsExistInOrbStand);
        Node dedupedRecords = mergedResult //
                .groupByAndLimit(new FieldList(mergedResult.getFieldNames()), 1);
        return dedupedRecords;
    }

}
