package com.latticeengines.dataflow.runtime.cascading.propdata;

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
                boolean shouldCreate = false;

                if (product.getProductBundle() != null) {
                    shouldCreate = true;
                    Tuple tuple = arguments.getTupleCopy();
                    tuple.set(positionMap.get(rolledUpFields.get(0)), product.getProductBundle());
                    tuple.set(positionMap.get(rolledUpFields.get(1)), ProductType.ANALYTIC.getName());
                    functionCall.getOutputCollector().add(tuple);
                }

                if (product.getProductLine() != null) {
                    shouldCreate = true;
                    Tuple tuple = arguments.getTupleCopy();
                    tuple.set(positionMap.get(rolledUpFields.get(0)), product.getProductLine());
                    tuple.set(positionMap.get(rolledUpFields.get(1)), ProductType.SPENDING.getName());
                    functionCall.getOutputCollector().add(tuple);
                }

                if (!shouldCreate) {
                    Tuple tuple = arguments.getTupleCopy();
                    tuple.set(positionMap.get(rolledUpFields.get(0)), productId);
                    tuple.set(positionMap.get(rolledUpFields.get(1)), ProductType.ANALYTIC.getName());
                    functionCall.getOutputCollector().add(tuple);
                }
            }
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
