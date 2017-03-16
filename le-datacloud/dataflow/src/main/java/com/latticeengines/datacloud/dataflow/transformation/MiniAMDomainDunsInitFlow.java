package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MiniAMDomainDunsInitConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("miniAMDomainDunsInitFlow")
public class MiniAMDomainDunsInitFlow extends ConfigurableFlowBase<MiniAMDomainDunsInitConfig> {
    private final static String DOMAIN_TYPE = "Domain";
    private final static String DUNS_TYPE = "Duns";
    private final static String GOLDEN_SET_DOMAIN = "Golden_Domain";
    private final static String GOLDEN_SET_DUNS = "Golden_Duns";
    @Override
    public Node construct(TransformationFlowParameters parameters) {
        List<String> miniAMAttr = getMiniAMAttr(parameters);
        List<FieldMetadata> fieldMetadataTmp = prepareMiniAMFieldMetadata(parameters);
        MiniAMDomainDunsInitConfig config = getTransformerConfig(parameters);
        Node goldenDataSet = addSource(parameters.getBaseTables().get(0));
        String checkGoldenSetDomain = config.getGoldenInputDataSetDomain() + " != null";
        String checkGoldenSetDuns = config.getGoldenInputDataSetDuns() + " != null";
        // Mapping GoldenSet domains from schema (Domain, Duns) to (Type, Value)
        Node goldenDataDomainSet = goldenDataSet
                .apply(String.format("%s ? %s : null", checkGoldenSetDomain, config.getGoldenInputDataSetDomain()), 
                        new FieldList(config.getGoldenInputDataSetDomain()), new FieldMetadata(GOLDEN_SET_DOMAIN, String.class)) //
                .retain(new FieldList(config.getGoldenInputDataSetDomain())) //
                .rename(new FieldList(config.getGoldenInputDataSetDomain()), new FieldList(config.getOutputDataSetValue())) //
                .addColumnWithFixedValue(config.getOutputDataSetType(), DOMAIN_TYPE, String.class) //
                .retain(new FieldList(miniAMAttr)) //
                .renamePipe("GoldenDomain");
        // Mapping GoldenSet duns from schema (Domain, Duns) to (Type, Value)
        Node goldenDataDunsSet = goldenDataSet
                .apply(String.format("%s ? %s : null", checkGoldenSetDuns, config.getGoldenInputDataSetDuns()),
                        new FieldList(config.getGoldenInputDataSetDuns()),
                        new FieldMetadata(GOLDEN_SET_DUNS, String.class)) //
                .retain(new FieldList(config.getGoldenInputDataSetDuns())) //
                .rename(new FieldList(config.getGoldenInputDataSetDuns()), new FieldList(config.getOutputDataSetValue())) //
                .addColumnWithFixedValue(config.getOutputDataSetType(), DUNS_TYPE, String.class) //
                .retain(new FieldList(miniAMAttr)) //
                .renamePipe("GoldenDuns");
        // Merge
        goldenDataDomainSet = goldenDataDomainSet.merge(goldenDataDunsSet);
        // Remove nulls
        String checkNullExpression = config.getOutputDataSetValue() + " != null ";
        goldenDataDomainSet = goldenDataDomainSet.filter(checkNullExpression,
                new FieldList(config.getOutputDataSetType(), config.getOutputDataSetValue()));
        // De-duplication
        goldenDataDomainSet = goldenDataDomainSet.groupByAndLimit(new FieldList(miniAMAttr), 1);
        return goldenDataDomainSet;
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
