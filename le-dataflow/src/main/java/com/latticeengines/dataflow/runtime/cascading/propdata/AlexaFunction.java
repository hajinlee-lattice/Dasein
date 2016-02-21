package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AlexaFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -1829655353767648353L;

    public static final String[] OUTPUT_FIELDS = new String[] { "US_PageViews", "US_Rank", "US_Users", "AU_PageViews",
            "AU_Rank", "AU_Users", "GB_PageViews", "GB_Rank", "GB_Users", "CA_PageViews", "CA_Rank", "CA_Users" };

    public AlexaFunction() {
        super(40, new Fields(OUTPUT_FIELDS));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(OUTPUT_FIELDS.length);
        for (int i = 1; i <= 10; i++) {
            String[] fields = inputGroup(i);
            String countryCode = arguments.getString(fields[0]);
            Double pageViews = arguments.getDouble(fields[1]);
            Integer rank = arguments.getInteger(fields[2]);
            Double users = arguments.getDouble(fields[3]);
            result = parseArgument(result, countryCode, pageViews, rank, users);
        }
        functionCall.getOutputCollector().add(result);
    }

    private String[] inputGroup(int idx) {
        String countryCode = String.format("Country%d_Code", idx);
        String pageViews = String.format("PageViews%d", idx);
        String rank = String.format("Rank%d", idx);
        String users = String.format("Users%d", idx);
        return new String[] { countryCode, pageViews, rank, users };
    }

    private Tuple parseArgument(Tuple result, String countryCode, Double pageViews, Integer rank, Double users) {
        Object[] output = new Object[OUTPUT_FIELDS.length];
        for (int i = 0; i < OUTPUT_FIELDS.length; i++) {
            output[i] = result.getObject(i);
        }
        if (StringUtils.isNotEmpty(countryCode)) {
            switch (countryCode.toUpperCase()) {
            case "US":
                output[0] = pageViews;
                output[1] = rank;
                output[2] = users;
                break;
            case "AU":
                output[3] = pageViews;
                output[4] = rank;
                output[5] = users;
                break;
            case "GB":
                output[6] = pageViews;
                output[7] = rank;
                output[8] = users;
                break;
            case "CA":
                output[9] = pageViews;
                output[10] = rank;
                output[11] = users;
                break;
            }
        }
        return new Tuple(output);
    }

}
