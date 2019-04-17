package com.latticeengines.datacloud.dataflow.transformation;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceSeedFileMergeTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("sourceSeedFileMergeFlow")
public class SourceSeedFileMergeFlow extends ConfigurableFlowBase<SourceSeedFileMergeTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SourceSeedFileMergeTransformerConfig config = getTransformerConfig(parameters);
        valiateParameters(parameters, config);

        Node source = addSource(parameters.getBaseTables().get(0));
        source = source.addColumnWithFixedValue(config.getSourceFieldName(), config.getSourceFieldValues().get(0),
                String.class);
        source = source.addColumnWithFixedValue(config.getSourcePriorityFieldName(),
                Integer.valueOf(config.getSourcePriorityFieldValues().get(0)), Integer.class);
        for (int i = 1; i < parameters.getBaseTables().size(); i++) {
            Node newSource = addSource(parameters.getBaseTables().get(i));
            newSource = newSource.addColumnWithFixedValue(config.getSourceFieldName(), config.getSourceFieldValues().get(i),
                    String.class);
            newSource = newSource.addColumnWithFixedValue(config.getSourcePriorityFieldName(),
                    Integer.valueOf(config.getSourcePriorityFieldValues().get(i)), Integer.class);
            source = source.merge(newSource);
        }
        return source;
    }

    private void valiateParameters(TransformationFlowParameters parameters, SourceSeedFileMergeTransformerConfig config) {
        if (parameters.getBaseTables().size() < 2) {
            throw new RuntimeException("There should be two or more sources!");
        }
        if (StringUtils.isBlank(config.getSourceFieldName())
                || StringUtils.isBlank(config.getSourcePriorityFieldName())) {
            throw new RuntimeException("Source field name or Source priority field name can not be blank!");
        }
        if (CollectionUtils.isEmpty(config.getSourceFieldValues())
                || CollectionUtils.isEmpty(config.getSourcePriorityFieldValues())) {
            throw new RuntimeException("Source field values or Source priority field values can not be empty!");
        }
        if (parameters.getBaseTables().size() != config.getSourceFieldValues().size()
                || parameters.getBaseTables().size() != config.getSourcePriorityFieldValues().size()) {
            throw new RuntimeException("Sources size is different from source field names' or source priorities'!");
        }

    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SourceSeedFileMergeTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "sourceSeedFileMergeFlow";
    }

    @Override
    public String getTransformerName() {
        return "sourceSeedFileMergeTransformer";

    }
}