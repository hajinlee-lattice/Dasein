package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.ProductMapperFlow.BEAN_NAME;

import java.util.List;
import java.util.Arrays;

import cascading.tuple.Fields;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ProductMapperFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(BEAN_NAME)
public class ProductMapperFlow extends ConfigurableFlowBase<ProductMapperConfig> {
    public static final String BEAN_NAME = "productMapperFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        ProductMapperConfig config = getTransformerConfig(parameters);
        Node node = addSource(parameters.getBaseTables().get(0));

        if (!node.getFieldNames().contains(config.getProductTypeField())) {
            node = node.addColumnWithFixedValue(config.getProductTypeField(), null, String.class);
        }

        List<String> rolledUpFields = Arrays.asList(config.getProductField(), config.getProductTypeField());

        Fields fieldDeclaration = new Fields(node.getFieldNamesArray());
        node = node.apply(
                new ProductMapperFunction(fieldDeclaration, config.getProductField(), config.getProductMap(), rolledUpFields), //
                new FieldList(node.getFieldNames()), //
                node.getSchema(), //
                new FieldList(node.getFieldNames()), //
                Fields.REPLACE);
        return node;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ProductMapperConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "productMapperFlow";
    }

    @Override
    public String getTransformerName() {
        return "productMapper";
    }
}
