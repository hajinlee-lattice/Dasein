package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.transaction.Product;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class ProductMapperFunction extends BaseOperation implements Function {

    private String productField;
    private Map<String, Product> productMap;

    public ProductMapperFunction(String productField, Map<String, Product> productMap) {
        super(new Fields(productField));
        this.productField = productField;
        this.productMap = productMap;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String productId = arguments.getString(productField).toString();
        Product product = productMap.get(productId);
        if ((product != null) && (product.getBundleId() != null)) {
            productId = product.getBundleId();
        }
        functionCall.getOutputCollector().add(new Tuple(productId));
    }

}
