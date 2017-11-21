package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ProductMapperFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("productMapperFlow")
public class ProductMapperFlow extends ConfigurableFlowBase<ProductMapperConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ProductMapperConfig config = getTransformerConfig(parameters);

        Node result = addSource(parameters.getBaseTables().get(0));
        result = result.apply(new ProductMapperFunction(config.getProductField(), config.getProductMap()),
                new FieldList(config.getProductField()), new FieldMetadata(config.getProductField(), String.class));
        return result;
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
