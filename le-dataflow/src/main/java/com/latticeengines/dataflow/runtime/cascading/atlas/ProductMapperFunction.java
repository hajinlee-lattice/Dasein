package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class ProductMapperFunction extends BaseOperation implements Function {

    private static final String PREFIX = "__";
    private String productField;
    private List<String> rolledUpFields;
    private Map<String, Integer> positionMap;

    public ProductMapperFunction(Fields fieldsDeclaration, String productField, List<String> rolledUpFields) {
        super(fieldsDeclaration);
        this.positionMap = getPositionMap(fieldsDeclaration);
        this.productField = productField;
        this.rolledUpFields = rolledUpFields;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple tuple = arguments.getTupleCopy();
        String productId = arguments.getString(PREFIX + productField);
        if (productId != null) {
            String typeName = arguments.getString(PREFIX + InterfaceName.ProductType.name());
            ProductType productType = ProductType.getProductType(typeName);
            switch (productType) {
            case Bundle:
                String bundleId = arguments.getString(PREFIX + InterfaceName.ProductBundleId.name());
                updateTuple(tuple, rolledUpFields, Arrays.asList(bundleId, ProductType.Analytic.name()));
                break;
            case Hierarchy:
                String line = arguments.getString(PREFIX + InterfaceName.ProductLine.name());
                String family = arguments.getString(PREFIX + InterfaceName.ProductFamily.name());
                String category = arguments.getString(PREFIX + InterfaceName.ProductCategory.name());
                if (line != null) {
                    updateTuple(tuple, rolledUpFields,
                            Arrays.asList(arguments.getString(PREFIX + InterfaceName.ProductLineId.name()),
                                    ProductType.Spending.name()));
                } else if (family != null) {
                    updateTuple(tuple, rolledUpFields,
                            Arrays.asList(arguments.getString(PREFIX + InterfaceName.ProductFamilyId.name()),
                                    ProductType.Spending.name()));
                } else if (category != null) {
                    updateTuple(tuple, rolledUpFields,
                            Arrays.asList(arguments.getString(PREFIX + InterfaceName.ProductCategoryId.name()),
                                    ProductType.Spending.name()));
                }
                break;
            case Analytic:
                updateTuple(tuple, rolledUpFields, Arrays.asList(productId, ProductType.Analytic.name()));
                break;
            case Spending:
                updateTuple(tuple, rolledUpFields, Arrays.asList(productId, ProductType.Spending.name()));
                break;
            default:
                break;
            }
        } else {
            updateTuple(tuple, rolledUpFields, Arrays.asList(Product.UNKNOWN_PRODUCT_ID, ProductType.Spending.name()));
        }
        functionCall.getOutputCollector().add(tuple);
    }

    private void updateTuple(Tuple tuple, List<String> fields, List<Object> values) {
        for (int i = 0; i < fields.size(); i++) {
            tuple.set(positionMap.get(fields.get(i)), values.get(i));
        }
    }

    private Map<String, Integer> getPositionMap(Fields fieldsDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldsDeclaration) {
            positionMap.put((String) field, pos++);
        }

        return positionMap;
    }
}
