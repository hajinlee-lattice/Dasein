package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ProductMapperFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;

import cascading.tuple.Fields;

@Component(ProductMapperFlow.BEAN_NAME)
public class ProductMapperFlow extends ConfigurableFlowBase<ProductMapperConfig> {
    private static final String PREFIX = "__";
    private static final Logger log = LoggerFactory.getLogger(ProductMapperFlow.class);
    public static final String BEAN_NAME = "productMapperFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        ProductMapperConfig config = getTransformerConfig(parameters);
        Node trxNode = addSource(parameters.getBaseTables().get(0));
        Node productNode = addSource(parameters.getBaseTables().get(1));
        log.info("Get productsfrom table=" + parameters.getBaseTables().get(1));

        if (!trxNode.getFieldNames().contains(config.getProductTypeField())) {
            trxNode = trxNode.addColumnWithFixedValue(config.getProductTypeField(), null, String.class);
        }

        List<String> rolledUpFields = Arrays.asList(config.getProductField(), config.getProductTypeField());
        productNode = productNode.filter(
                String.format("\"%s\".equals(%s)", ProductStatus.Active.name(), InterfaceName.ProductStatus.name()),
                new FieldList(InterfaceName.ProductStatus.name()));
        productNode = renameColumns(productNode, config);

        Node result = trxNode.leftJoin(new FieldList(config.getProductField()), productNode,
                new FieldList(PREFIX + config.getProductField()));
        Fields fieldDeclaration = new Fields(result.getFieldNamesArray());
        result = result.apply(new ProductMapperFunction(fieldDeclaration, config.getProductField(), rolledUpFields), //
                new FieldList(result.getFieldNames()), //
                result.getSchema(), //
                new FieldList(result.getFieldNames()), //
                Fields.REPLACE);
        result = result.retain(new FieldList(trxNode.getFieldNames()));
        return result;
    }

    private Node renameColumns(Node productNode, ProductMapperConfig config) {
        List<String> fields = productNode.getFieldNames();
        if (!fields.contains(config.getProductTypeField())) {
            productNode = productNode.addColumnWithFixedValue(config.getProductTypeField(),
                    ProductType.Raw.name(), String.class);
            fields = productNode.getFieldNames();
        }
        List<String> productFields = fields;
        List<String> predefinedFields = Arrays.asList( //
                config.getProductField(), //
                config.getProductTypeField(), //
                InterfaceName.ProductBundleId.name(), //
                InterfaceName.ProductLine.name(), //
                InterfaceName.ProductLineId.name(), //
                InterfaceName.ProductFamily.name(), //
                InterfaceName.ProductFamilyId.name(), //
                InterfaceName.ProductCategory.name(), //
                InterfaceName.ProductCategoryId.name() //
        );
        predefinedFields = predefinedFields.stream().filter(f -> productFields.contains(f))
                .collect(Collectors.toList());
        List<String> newNames = new ArrayList<>();
        predefinedFields.forEach(f -> newNames.add(PREFIX + f));
        productNode = productNode.rename(new FieldList(predefinedFields), new FieldList(newNames));
        productNode = productNode.retain(new FieldList(productNode.getFieldNames()));
        return productNode;
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
