package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MiniAMSeedSampleSetConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("miniAmSeedSampleSetFlow")
public class MiniAMSeedSampleSetFlow extends ConfigurableFlowBase<MiniAMSeedSampleSetConfig> {

    private static final String DOMAIN_TYPE = "Domain";
    private static final String DUNS_TYPE = "Duns";

    @Override
    public String getDataFlowBeanName() {
        return "miniAmSeedSampleSetFlow";
    }

    @Override
    public String getTransformerName() {
        return "miniAMSampledSetTransformer";
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return MiniAMSeedSampleSetConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        MiniAMSeedSampleSetConfig config = getTransformerConfig(parameters);
        Node miniDataUniverse = addSource(parameters.getBaseTables().get(0));
        Node seedDataSet = addSource(parameters.getBaseTables().get(1));
        String filterDomain = config.getMiniDataSetType() + ".equals(\"" + DOMAIN_TYPE + "\")";
        String filterDuns = config.getMiniDataSetType() + ".equals(\"" + DUNS_TYPE + "\")";

        // Filtering domain
        Node miniDomainSampledSet = miniDataUniverse //
                .filter(filterDomain, new FieldList(config.getMiniDataSetType())); //

        // Filtering duns
        Node miniDunsSampledSet = miniDataUniverse //
                .filter(filterDuns, new FieldList(config.getMiniDataSetType()));

        // Matching domain
        Node miniSampledSetByDomain = seedDataSet //
                .join(new FieldList(config.getSampledSetDomain()), miniDomainSampledSet,
                        new FieldList(config.getMiniDataSetValue()), JoinType.INNER, false) //
                .retain(new FieldList(seedDataSet.getFieldNames()));

        // Matching duns
        Node miniSampledSetByDuns = seedDataSet //
                .join(new FieldList(config.getSampledSetDuns()), miniDunsSampledSet,
                        new FieldList(config.getMiniDataSetValue()), JoinType.INNER, false) //
                .retain(new FieldList(seedDataSet.getFieldNames()));

        // Merge domains and duns set
        Node finalMergedSet = miniSampledSetByDomain.merge(miniSampledSetByDuns);
        // De-duplication
        finalMergedSet = finalMergedSet.groupByAndLimit(new FieldList(config.getKeyIdentifier()), 1);

        return finalMergedSet;
    }

}
