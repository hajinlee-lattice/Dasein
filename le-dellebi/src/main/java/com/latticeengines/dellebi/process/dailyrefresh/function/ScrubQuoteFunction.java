package com.latticeengines.dellebi.process.dailyrefresh.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class ScrubQuoteFunction extends BaseOperation implements Function {

	private static final long serialVersionUID = 4333077483123704366L;

	public ScrubQuoteFunction(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    private static final Log log = LogFactory.getLog(ScrubQuoteFunction.class);

    
	@Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall ) {
        TupleEntry argument = functionCall.getArguments();

        HadoopFlowProcess hfp = (HadoopFlowProcess) flowProcess;
        MultiInputSplit mis = (MultiInputSplit) hfp.getReporter().getInputSplit();
        FileSplit fs = (FileSplit) mis.getWrappedInputSplit();
        String fileName = fs.getPath().getName();

        String quoteCreationDate = convertDatetimeToDate(argument.getString("QUOTE_CREATE_DATE"));

        Tuple result = new Tuple();
        result.add(argument.getString("#QTE_NUM_VAL"));
        result.add(quoteCreationDate);
        result.add(argument.getString("SLDT_CUST_NUM_VAL"));
        result.add(argument.getString("ITM_NUM_VAL"));
        result.add(argument.getString("LEAD_SLS_REP_ASSOC_BDGE_NBR"));
        result.add(argument.getString("SYS_QTY"));
        result.add(argument.getString("REVN_USD_AMT"));
        result.add(argument.getString("SLDT_BU_ID"));
        result.add(fileName);

        functionCall.getOutputCollector().add(result);

    }

    private String convertDatetimeToDate(String s) {
        if (s != null && !s.isEmpty() && s.trim().length() > 10) {
            SimpleDateFormat formatterOld = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
            SimpleDateFormat formatterNew = new SimpleDateFormat("yyyy-MM-dd");
            Date date;
            try {
                date = formatterOld.parse(s);
                s = formatterNew.format(date);
            } catch (ParseException e) {
                log.error("Failed to convert " + s + " to Date.");
                log.error("Failed!", e);
            }

        } else {
            return "";
        }
        return s;
    }

}
