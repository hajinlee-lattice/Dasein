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

@SuppressWarnings("rawtypes")
public class ScrubOdrSumFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -3364175814238773294L;

    public ScrubOdrSumFunction(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        HadoopFlowProcess hfp = (HadoopFlowProcess) flowProcess;
        MultiInputSplit mis = (MultiInputSplit) hfp.getReporter().getInputSplit();
        FileSplit fs = (FileSplit) mis.getWrappedInputSplit();
        String fileName = fs.getPath().getName();

        TupleEntry argument = functionCall.getArguments();

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();

        String stageDate = dateFormat.format(cal.getTime());
        String processedFlg = "0";

        Tuple result = new Tuple();

        for (int i = 0; i < argument.size(); i++) {
            result.add(argument.getString(i));
        }

        // Add additional fields.
        result.add(processedFlg);
        result.add(stageDate);
        result.add(fileName);

        functionCall.getOutputCollector().add(result);
    }
}
