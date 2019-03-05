package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ProductMapperFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.util.ProductUtils;

import cascading.tuple.Fields;

@Component(ProductMapperFlow.BEAN_NAME)
public class ProductMapperFlow extends ConfigurableFlowBase<ProductMapperConfig> {
    private static final Logger log = LoggerFactory.getLogger(ProductMapperFlow.class);
    public static final String BEAN_NAME = "productMapperFlow";

    @Inject
    private Configuration yarnConfiguration;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        ProductMapperConfig config = getTransformerConfig(parameters);
        Node node = addSource(parameters.getBaseTables().get(0));

        if (!node.getFieldNames().contains(config.getProductTypeField())) {
            node = node.addColumnWithFixedValue(config.getProductTypeField(), null, String.class);
        }

        List<String> rolledUpFields = Arrays.asList(config.getProductField(), config.getProductTypeField());
        Fields fieldDeclaration = new Fields(node.getFieldNamesArray());

        Map<String, List<Product>> productMap;
        if (config.getProductTable() == null && config.getProductMap() != null) {
            log.info("Get product map from config=" + JsonUtils.serialize(config));
            productMap = config.getProductMap();
        } else {
            Table table = config.getProductTable();
            log.info("Get product map from table=" + table.getName());
            List<Product> productList = new ArrayList<>();
            table.getExtracts().forEach(
                    extract -> productList.addAll(ProductUtils.loadProducts(yarnConfiguration, extract.getPath())));
            productMap = ProductUtils.getActiveProductMap(productList);
        }

        node = node.apply(
                new ProductMapperFunction(fieldDeclaration, config.getProductField(), productMap, rolledUpFields), //
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
        return ProductMapperFlow.BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PRODUCT_MAPPER;
    }
}
