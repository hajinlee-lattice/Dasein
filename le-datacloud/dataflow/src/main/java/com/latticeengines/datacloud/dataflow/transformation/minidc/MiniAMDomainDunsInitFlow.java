package com.latticeengines.datacloud.dataflow.transformation.minidc;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.minidc.MiniAMDomainDunsInitConfig;

/**
 * A pipeline step of mini DataCloud creation pipeline
 * https://confluence.lattice-engines.com/display/ENG/AccountMaster+Rebuild+Pipelines#AccountMasterRebuildPipelines-MiniAMDomainDunsCreation
 */
@Component(MiniAMDomainDunsInitFlow.DATAFLOW_BEAN)
public class MiniAMDomainDunsInitFlow extends ConfigurableFlowBase<MiniAMDomainDunsInitConfig> {

    public static final String TRANSFORMER = "miniDnbAMDomainDunsTransformer";
    public static final String DATAFLOW_BEAN = "miniAMDomainDunsInitFlow";

    private static final String DOMAIN_TYPE = "Domain";
    private static final String DUNS_TYPE = "Duns";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        List<String> miniAMAttr = getMiniAMAttr(parameters);
        MiniAMDomainDunsInitConfig config = getTransformerConfig(parameters);
        List<Node> storeList = new ArrayList<>();
        for (int i = 0; i < parameters.getBaseTables().size(); i++) {
            Node goldenDataSet = addSource(parameters.getBaseTables().get(i));
            // Renaming source name to pipeName to ensure they are consistent
            goldenDataSet.renamePipe(parameters.getBaseTables().get(i));
            storeList.add(goldenDataSet);
        }
        Node goldenSetResult = null;
        for (Node goldenSet : storeList) {
            String checkGoldenSetDomain = config.getGoldenInputDataSetDomain().get(goldenSet.getPipeName())
                    + " != null";
            String checkGoldenSetDuns = config.getGoldenInputDataSetDuns().get(goldenSet.getPipeName()) + " != null";
            /*
             * Mapping GoldenSet domains from schema (Domain, Duns) to (Type,
             * Value)
             */
            Node goldenDataDomainSet = goldenSet
                    .filter(checkGoldenSetDomain,
                            new FieldList(config.getGoldenInputDataSetDomain().get(goldenSet.getPipeName()))) //
                    .retain(new FieldList(config.getGoldenInputDataSetDomain().get(goldenSet.getPipeName()))) //
                    .rename(new FieldList(config.getGoldenInputDataSetDomain().get(goldenSet.getPipeName())),
                            new FieldList(config.getOutputDataSetValue())) //
                    .addColumnWithFixedValue(config.getOutputDataSetType(), DOMAIN_TYPE, String.class) //
                    .renamePipe("GoldenDomain");
            /*
             * Mapping GoldenSet duns from schema (Domain, Duns) to (Type,
             * Value)
             */
            Node goldenDataDunsSet = goldenSet
                    .filter(checkGoldenSetDuns,
                            new FieldList(config.getGoldenInputDataSetDuns().get(goldenSet.getPipeName()))) //
                    .retain(new FieldList(config.getGoldenInputDataSetDuns().get(goldenSet.getPipeName()))) //
                    .rename(new FieldList(config.getGoldenInputDataSetDuns().get(goldenSet.getPipeName())),
                            new FieldList(config.getOutputDataSetValue())) //
                    .addColumnWithFixedValue(config.getOutputDataSetType(), DUNS_TYPE, String.class) //
                    .renamePipe("GoldenDuns");
            // Merge
            goldenDataDomainSet = goldenDataDomainSet.merge(goldenDataDunsSet);
            // Remove nulls
            String checkNullExpression = config.getOutputDataSetValue() + " != null && " + "!"
                    + config.getOutputDataSetValue() + ".equals(\"\")";
            goldenDataDomainSet = goldenDataDomainSet.filter(checkNullExpression,
                    new FieldList(config.getOutputDataSetValue()));

            // De-duplication
            goldenDataDomainSet = goldenDataDomainSet.groupByAndLimit(new FieldList(miniAMAttr), 1);
            goldenSetResult = goldenDataDomainSet;
        }
        return goldenSetResult;
    }

    private List<String> getMiniAMAttr(TransformationFlowParameters parameters) {
        MiniAMDomainDunsInitConfig config = getTransformerConfig(parameters);
        List<String> miniAMAttrs = new ArrayList<String>();
        miniAMAttrs.add(config.getOutputDataSetType());
        miniAMAttrs.add(config.getOutputDataSetValue());
        return miniAMAttrs;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return MiniAMDomainDunsInitConfig.class;
    }
}
