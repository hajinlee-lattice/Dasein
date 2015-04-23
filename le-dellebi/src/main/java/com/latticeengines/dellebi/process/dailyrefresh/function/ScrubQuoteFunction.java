package com.latticeengines.dellebi.process.dailyrefresh.function;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.mapred.FileSplit;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tap.hadoop.io.MultiInputSplit;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ScrubQuoteFunction extends BaseOperation implements Function {

    public ScrubQuoteFunction(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry argument = functionCall.getArguments();
        String processdFlg = "0";

        Tuple result = new Tuple();
        result.add(argument.getString("#QTE_NUM_VAL"));
        result.add(argument.getString("QTE_LN_NUM"));
        
        /*for (int i = 0; i < argument.size(); i++) {
            result.add(argument.getString(i));
        }*/

        // Add additional fields.
        result.add(processdFlg);

        functionCall.getOutputCollector().add(result);

    }

}
