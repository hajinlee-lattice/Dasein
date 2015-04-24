package com.latticeengines.dellebi.process.dailyrefresh.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ScrubQuoteFunction extends BaseOperation implements Function {

    public ScrubQuoteFunction(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }
    private static final Logger LOGGER = Logger.getLogger(ScrubQuoteFunction.class);
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry argument = functionCall.getArguments();
        String processdFlg = "0";
        
        String quoteCreationDate = convertDatetimeToDate(argument.getString("QUOTE_CREATE_DATE"));

        Tuple result = new Tuple();
        result.add(argument.getString("#QTE_NUM_VAL"));
        result.add(quoteCreationDate);
        result.add(argument.getString("SLDT_CUST_NUM_VAL"));
        result.add(argument.getString("ITM_NUM_VAL"));
        result.add(argument.getString("REVN_USD_AMT"));
        result.add(argument.getString("SYS_QTY"));
        result.add(argument.getString("LEAD_SLS_REP_ASSOC_BDGE_NBR"));
        result.add(processdFlg);

        functionCall.getOutputCollector().add(result);

    }
    
    private String convertDatetimeToDate(String s) {
        if (s != null && !s.isEmpty() && s.trim().length()>10){
            SimpleDateFormat formatterOld = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
            SimpleDateFormat formatterNew = new SimpleDateFormat("yyyy-MM-dd");
            Date date;
            try {
                date = formatterOld.parse(s);
                s = formatterNew.format(date);
            } catch (ParseException e) {
                LOGGER.error("Failed to convert " + s + " to Date."); 
                LOGGER.error("Failed!", e);
            }
            
        }else{
            return "";
        }
        return s;
    }

}
