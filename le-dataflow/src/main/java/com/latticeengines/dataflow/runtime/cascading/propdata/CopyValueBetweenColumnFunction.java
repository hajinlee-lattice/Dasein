package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class CopyValueBetweenColumnFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 704139437526099546L;

    private static Logger LOG = LogManager.getLogger(CopyValueBetweenColumnFunction.class);

    private Map<String, String> inputColumnMapping; // input - > output
    private Map<String, String> outputColumnMapping; // output -> input
    private String[] outputColumns;

    public CopyValueBetweenColumnFunction(Map<String, String> intputColumnMapping,
            String[] outputColumns) {
        super(new Fields(outputColumns));
        this.inputColumnMapping = intputColumnMapping;
        this.outputColumns = outputColumns;
        for (String outputColumn : outputColumns) {
            this.outputColumnMapping.put(outputColumn, this.inputColumnMapping.get(outputColumn));
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(outputColumns.length);
        Object[] output = new Object[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) {
            output[i] = result.getObject(i);
            String outputColumn = outputColumns[i];
            String inputColumn = outputColumnMapping.get(outputColumn);
            String inputValue = arguments.getString(inputColumn);
            LOG.info(inputColumn + "->" + outputColumn + ": " + inputValue);
            output[i] = inputValue;
        }
        result = new Tuple(output);
        functionCall.getOutputCollector().add(result);
    }
}
