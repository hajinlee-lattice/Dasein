package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DnBCleanConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(DnBCleanFlow.DATAFLOW_BEAN_NAME)
public class DnBCleanFlow extends ConfigurableFlowBase<DnBCleanConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DnBCleanFLow";

    public static final String TRANSFORMER_NAME = "DnBCleanTransformer";

    private DnBCleanConfig config;

    private static final String NEW = "NEW_";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));
        FieldMetadata salesFm = source.getSchema(config.getSalesVolumeUSField());
        source = source.apply(String.format("(%s == 0 && (%s == null || %s.equals(\"2\"))) ? null : %s",
                config.getSalesVolumeUSField(), config.getSalesVolumeCodeField(), config.getSalesVolumeCodeField(),
                config.getSalesVolumeUSField()),
                new FieldList(config.getSalesVolumeUSField(), config.getSalesVolumeCodeField()),
                new FieldMetadata(NEW + config.getSalesVolumeUSField(), salesFm.getJavaType()))
                .discard(new FieldList(config.getSalesVolumeUSField()))
                .rename(new FieldList(NEW + config.getSalesVolumeUSField()),
                        new FieldList(config.getSalesVolumeUSField()));
        FieldMetadata empHereFm = source.getSchema(config.getEmployeeHereField());
        source = source.apply(String.format("(%s == 0 && (%s == null || %s.equals(\"2\"))) ? null : %s",
                config.getEmployeeHereField(), config.getEmployeeHereCodeField(), config.getEmployeeHereCodeField(),
                config.getEmployeeHereField()),
                new FieldList(config.getEmployeeHereField(), config.getEmployeeHereCodeField()),
                new FieldMetadata(NEW + config.getEmployeeHereField(), empHereFm.getJavaType()))
                .discard(new FieldList(config.getEmployeeHereField()))
                .rename(new FieldList(NEW + config.getEmployeeHereField()),
                        new FieldList(config.getEmployeeHereField()));
        FieldMetadata empTotalFm = source.getSchema(config.getEmployeeTotalField());
        source = source.apply(String.format("(%s == 0 && (%s == null || %s.equals(\"2\"))) ? null : %s",
                config.getEmployeeTotalField(), config.getEmployeeTotalCodeField(), config.getEmployeeTotalCodeField(),
                config.getEmployeeTotalField()),
                new FieldList(config.getEmployeeTotalField(), config.getEmployeeTotalCodeField()),
                new FieldMetadata(NEW + config.getEmployeeTotalField(), empTotalFm.getJavaType()))
                .discard(new FieldList(config.getEmployeeTotalField()))
                .rename(new FieldList(NEW + config.getEmployeeTotalField()),
                        new FieldList(config.getEmployeeTotalField()));
        return source;
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
        return DnBCleanConfig.class;
    }
}
