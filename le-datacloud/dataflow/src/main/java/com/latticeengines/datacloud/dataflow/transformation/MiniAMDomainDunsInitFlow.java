package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.MiniAMSeedMergeDomainDuns;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MiniAMDomainDunsInitConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("miniAMDomainDunsInitFlow")
public class MiniAMDomainDunsInitFlow extends ConfigurableFlowBase<MiniAMDomainDunsInitConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        List<String> miniAMAttr = getMiniAMAttr(parameters);
        List<FieldMetadata> fieldMetadataTmp = prepareMiniAMFieldMetadata(parameters);
        MiniAMDomainDunsInitConfig config = getTransformerConfig(parameters);
        Node goldenDataSet = addSource(parameters.getBaseTables().get(0));
        goldenDataSet = goldenDataSet.apply(
                new MiniAMSeedMergeDomainDuns(new Fields(miniAMAttr.toArray(new String[miniAMAttr.size()])),
                        config.getGoldenInputDataSetDomain(), config.getGoldenInputDataSetDuns(),
                        config.getOutputDataSetType(), config.getOutputDataSetValue()),
                new FieldList(goldenDataSet.getFieldNames()), fieldMetadataTmp, new FieldList(miniAMAttr));
        // De-duplication
        goldenDataSet = goldenDataSet.groupByAndLimit(new FieldList(miniAMAttr), 1);
        return goldenDataSet;
    }

    private List<FieldMetadata> prepareMiniAMFieldMetadata(TransformationFlowParameters parameters) {
        MiniAMDomainDunsInitConfig config = getTransformerConfig(parameters);
        List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>();
        fieldMetadata.add(new FieldMetadata(config.getOutputDataSetType(), String.class));
        fieldMetadata.add(new FieldMetadata(config.getOutputDataSetValue(), String.class));
        return fieldMetadata;
    }

    private List<String> getMiniAMAttr(TransformationFlowParameters parameters) {
        MiniAMDomainDunsInitConfig config = getTransformerConfig(parameters);
        List<String> miniAMAttrsTmp = new ArrayList<String>();
        miniAMAttrsTmp.add(config.getOutputDataSetType());
        miniAMAttrsTmp.add(config.getOutputDataSetValue());
        return miniAMAttrsTmp;
    }

    @Override
    public String getDataFlowBeanName() {
        return "miniAMDomainDunsInitFlow";
    }

    @Override
    public String getTransformerName() {
        return "miniDnbAMDomainDunsTransformer";
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return MiniAMDomainDunsInitConfig.class;
    }
}
