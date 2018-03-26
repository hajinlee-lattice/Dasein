package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;

@SuppressWarnings({ "rawtypes", "serial" })
public class ProductMapperFunction extends BaseOperation implements Function {

    private String productField;
    private Map<String, List<Product>> productMap;
    private List<String> rolledUpFields;
    private Map<String, Integer> positionMap;

    public ProductMapperFunction(Fields fieldsDeclaration, String productField, Map<String, List<Product>> productMap,
                                 List<String> rolledUpFields) {
        super(fieldsDeclaration);
        this.positionMap = getPositionMap(fieldsDeclaration);
        this.productField = productField;
        this.rolledUpFields = rolledUpFields;
        this.productMap = productMap;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String productId = arguments.getString(productField);
        List<Product> products = productMap.get(productId);

        if (products != null) {
            for (Product product : products) {
                Tuple tuple = arguments.getTupleCopy();
                switch (ProductType.valueOf(product.getProductType())) {
                    case Bundle:
                        updateTuple(tuple, rolledUpFields,
                                Arrays.asList(product.getProductBundleId(), ProductType.Analytic.name()));
                        functionCall.getOutputCollector().add(tuple);
                        break;
                    case Hierarchy:
                        if (product.getProductLine() != null) {
                            updateTuple(tuple, rolledUpFields,
                                    Arrays.asList(product.getProductLineId(), ProductType.Spending.name()));
                            functionCall.getOutputCollector().add(tuple);
                        } else if (product.getProductFamily() != null) {
                            updateTuple(tuple, rolledUpFields,
                                    Arrays.asList(product.getProductFamilyId(), ProductType.Spending.name()));
                            functionCall.getOutputCollector().add(tuple);
                        } else if (product.getProductCategory() != null) {
                            updateTuple(tuple, rolledUpFields,
                                    Arrays.asList(product.getProductCategoryId(), ProductType.Spending.name()));
                            functionCall.getOutputCollector().add(tuple);
                        }
                        break;
                    case Analytic:
                        updateTuple(tuple, rolledUpFields,
                                Arrays.asList(product.getProductId(), ProductType.Analytic.name()));
                        functionCall.getOutputCollector().add(tuple);
                        break;
                    case Spending:
                        updateTuple(tuple, rolledUpFields,
                                Arrays.asList(product.getProductId(), ProductType.Spending.name()));
                        functionCall.getOutputCollector().add(tuple);
                        break;
                    default:
                        break;
                }
            }
        }
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
