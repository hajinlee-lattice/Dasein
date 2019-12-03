package com.latticeengines.datacloud.dataflow.transformation;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AttrHasPurchasedFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AttrMarginFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PurchaseAttributesDeriverConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component(PurchaseAttributesDeriverFlow.FLOW_BEAN_NAME)
public class PurchaseAttributesDeriverFlow extends ConfigurableFlowBase<PurchaseAttributesDeriverConfig> {
    public static final String FLOW_BEAN_NAME = "purchaseAttributesDeriverFlow";
    private static final String UNSUPPORTED_MSG_FMT = "Field %s is unsupported by transformer %s.";
    private Map<InterfaceName, Class<?>> fieldTypes;

    @PostConstruct
    private void postConstruct() {
        fieldTypes = new HashMap<>();
        fieldTypes.put(InterfaceName.Margin, Double.class);
        fieldTypes.put(InterfaceName.HasPurchased, Boolean.class);
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PurchaseAttributesDeriverConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return PurchaseAttributesDeriverFlow.FLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.TRANSFORMER_ATTRIBUTES_DERIVER;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        PurchaseAttributesDeriverConfig config = getTransformerConfig(parameters);
        Node node = addSource(parameters.getBaseTables().get(0));

        for (InterfaceName field : config.getFields()) {
            if (!fieldTypes.containsKey(field)) {
                throw new UnsupportedOperationException(
                        String.format(UNSUPPORTED_MSG_FMT, field, config.getTransformer()));
            }

            Class<?> fieldType = fieldTypes.get(field);

            switch (field) {
                case Margin:
                    node = node.apply(
                            new AttrMarginFunction(field),
                            new FieldList(node.getFieldNames()),
                            new FieldMetadata(field.name(), fieldType));
                    break;
                case HasPurchased:
                    node = node.apply(
                        new AttrHasPurchasedFunction(field.name(), true),
                            new FieldList(node.getFieldNames()),
                            new FieldMetadata(field.name(), fieldType));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(UNSUPPORTED_MSG_FMT, field.name(), config.getTransformer()));
            }
        }

        return node;
    }
}
