package com.latticeengines.dataflow.runtime.cascading;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.transform.exposed.RealTimeTransform;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class TransformFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(TransformFunction.class);

    private static final long serialVersionUID = -511307280951632248L;

    private TransformDefinition definition;

    private RealTimeTransform transform;

    protected TransformFunction(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    public TransformFunction(TransformDefinition definition, RealTimeTransform transform,
            Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.definition = definition;
        this.transform = transform;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleArguments = functionCall.getArguments();
        Fields fields = tupleArguments.getFields();
        Tuple tuple = tupleArguments.getTuple();

        Map<String, Object> record = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Object value = tuple.getObject(i);
            record.put(String.valueOf(fields.get(i)), value);
        }

        Map<String, Object> arguments = definition.arguments;
        Object value = transform.transform(arguments, record);
        if (value == null) {
            value = null;
        } else if (definition.type.type() == Double.class) {
            try {
                if (value.toString().toLowerCase().equals("true") == true) {
                    value = definition.type.type().cast(Double.valueOf("1.0"));
                } else if (value.toString().toLowerCase().equals("false") == true) {
                    value = definition.type.type().cast(Double.valueOf("0.0"));
                } else if (value.toString().equals("null") == false
                        && value.toString().equals("None") == false) {
                    value = definition.type.type().cast(Double.valueOf(value.toString()));
                } else {
                    value = null;
                }
            } catch (Exception e) {
                log.warn(String.format("Problem casting Transform value to Java Double"));
            }
        } else if (definition.type.type() == Long.class) {
            value = Long.valueOf(value.toString());
        }

        if (value != null) {
            functionCall.getOutputCollector().add(new Tuple(value));
        } else {
            functionCall.getOutputCollector().add(Tuple.size(1));
        }

    }
}
