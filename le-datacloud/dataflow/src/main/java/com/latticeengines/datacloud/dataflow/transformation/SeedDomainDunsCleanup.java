package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SeedDomainDunsCleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(SeedDomainDunsCleanup.DATAFLOW_BEAN_NAME)
public class SeedDomainDunsCleanup extends ConfigurableFlowBase<SeedDomainDunsCleanupConfig> {

    public static final String DATAFLOW_BEAN_NAME = "SeedDomainDunsCleanup";
    public static final String TRANSFORMER_NAME = "SeedDomainDunsCleanupTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        SeedDomainDunsCleanupConfig config = getTransformerConfig(parameters);

        Node seed = addSource(parameters.getBaseTables().get(0));
        Node goldenDom = addSource(parameters.getBaseTables().get(1));
        Node goldenDuns = addSource(parameters.getBaseTables().get(2));

        goldenDom = goldenDom.retain(new FieldList(config.getGoldenDomainField()))
                .filter(String.format("%s != null && !%s.equals(\"\")", config.getGoldenDomainField(),
                        config.getGoldenDomainField()),
                        new FieldList(config.getGoldenDomainField()))
                .rename(new FieldList(config.getGoldenDomainField()),
                        new FieldList(renameGolden(config.getGoldenDomainField())));
        goldenDuns = goldenDuns.retain(new FieldList(config.getGoldenDunsField()))
                .filter(String.format("%s != null && !%s.equals(\"\")", config.getGoldenDunsField(),
                        config.getGoldenDunsField()),
                        new FieldList(config.getGoldenDunsField()))
                .rename(new FieldList(config.getGoldenDunsField()),
                        new FieldList(renameGolden(config.getGoldenDunsField())));

        Node joined = seed.join(new FieldList(config.getSeedDomainField()), goldenDom,
                new FieldList(renameGolden(config.getGoldenDomainField())), JoinType.LEFT);
        joined = joined
                .filter(String.format("%s == null", renameGolden(config.getGoldenDomainField())),
                        new FieldList(renameGolden(config.getGoldenDomainField())))
                .retain(new FieldList(seed.getFieldNames()));
        joined = joined.join(new FieldList(config.getSeedDunsField()), goldenDuns,
                new FieldList(renameGolden(config.getGoldenDunsField())), JoinType.LEFT);
        joined = joined
                .filter(String.format("%s == null", renameGolden(config.getGoldenDunsField())),
                        new FieldList(renameGolden(config.getGoldenDunsField())))
                .retain(new FieldList(seed.getFieldNames()));
        return joined;
    }

    private String renameGolden(String field) {
        return "_GOLDEN_" + field;
    }

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
        return SeedDomainDunsCleanupConfig.class;
    }
}
