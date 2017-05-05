package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMStatsHQDunsFunction extends BaseOperation<Map> //
        implements Function<Map> {
    private static final long serialVersionUID = -4039806083023012431L;
    private static final Log log = LogFactory.getLog(AMStatsHQDunsFunction.class);

    private String subIndicatorField;
    private String statusCodeField;
    private String dunsField;
    private String ddunsField;
    private String gdunsField;
    private String hqDunsField;

    private int subIndicatorFieldLoc;
    private int statusCodeFieldLoc;
    private int dunsFieldLoc;
    private int ddunsFieldLoc;
    private int gdunsFieldLoc;
    private int hqDunsFieldLoc;

    public AMStatsHQDunsFunction(Params params) {
        super(params.outputFieldsDeclaration);

        subIndicatorField = params.subIndicatorField;
        statusCodeField = params.statusCodeField;
        dunsField = params.dunsField;
        ddunsField = params.ddunsField;
        gdunsField = params.gdunsField;
        hqDunsField = params.hqDunsField;

        subIndicatorFieldLoc = params.outputFieldsDeclaration.getPos(subIndicatorField);
        statusCodeFieldLoc = params.outputFieldsDeclaration.getPos(statusCodeField);
        dunsFieldLoc = params.outputFieldsDeclaration.getPos(dunsField);
        ddunsFieldLoc = params.outputFieldsDeclaration.getPos(ddunsField);
        gdunsFieldLoc = params.outputFieldsDeclaration.getPos(gdunsField);
        hqDunsFieldLoc = params.outputFieldsDeclaration.getPos(hqDunsField);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Map> functionCall) {
        TupleEntry entry = functionCall.getArguments();

        Tuple result = entry.getTupleCopy();

        Fields fields = entry.getFields();

        int idx = 0;
        String[] fieldNames = new String[] { subIndicatorField, statusCodeField, //
                dunsField, ddunsField, gdunsField, hqDunsField };
        Integer[] fieldLocs = new Integer[] { subIndicatorFieldLoc, statusCodeFieldLoc, //
                dunsFieldLoc, ddunsFieldLoc, gdunsFieldLoc, hqDunsFieldLoc };

        for (String field : fieldNames) {
            int loc = fieldLocs[idx++];
            if (!field.equalsIgnoreCase((String) fields.get(loc))) {
                throw new RuntimeException("Field index mismatch: " + field);
            }
        }

        String subIndicator = (String) entry.getObject(subIndicatorFieldLoc);
        String statusCode = (String) entry.getObject(statusCodeFieldLoc);

        if (subIndicator == null || statusCode == null) {
            throw new RuntimeException("subIndicator and statusCode both should not be null ");
        }

        String duns = (String) entry.getObject(dunsFieldLoc);
        String dduns = (String) entry.getObject(ddunsFieldLoc);
        String gduns = (String) entry.getObject(gdunsFieldLoc);
        String hqDuns = null;

        try {
            hqDuns = calculateHQDuns(Integer.parseInt(subIndicator), //
                    Integer.parseInt(statusCode), //
                    duns, dduns, gduns);
        } catch (NumberFormatException e) {
            log.error("subIndicator = " + subIndicator + ", statusCode = " + statusCode
                    + " should both be valid numbers: " + e.getMessage());
        }
        result.set(hqDunsFieldLoc, hqDuns);
        functionCall.getOutputCollector().add(result);
    }

    private String calculateHQDuns(int subIndicator, int statusCode, //
            String duns, String dduns, String gduns) {
        String hqDuns = null;

        if (statusCode == 0 && subIndicator == 0) {
            // Site type - Stand Alone Business
            // Site D-U-N-S = DDUNS = HQ DUNS

            hqDuns = duns;
        } else if (statusCode == 1 && subIndicator == 0) {
            // Site type - Global HQ
            // GDUNS = DDUNS = HQ DUNS

            hqDuns = (dduns == null) ? gduns : dduns;
        } else if (statusCode == 1 && subIndicator == 3) {
            // Site type - Subsidiary
            // Site D-U-N-S = HQ DUNS

            hqDuns = duns;
        } else if (statusCode == 0 && subIndicator == 3) {
            // Site type - Single Site Subsidiary
            // If Site D-U-N-S = DDUNS, the HQ DUNS

            if (duns.equalsIgnoreCase(dduns)) {
                hqDuns = duns;
            }
        }

        return hqDuns;
    }

    public static class Params {
        Fields outputFieldsDeclaration;
        String statusCodeField;
        String subIndicatorField;
        String dunsField;
        String ddunsField;
        String gdunsField;
        String hqDunsField;

        public Params(Fields outputFieldsDeclaration, String statusCodeField, //
                String subIndicatorField, String dunsField, String ddunsField, //
                String gdunsField, String hqDunsField) {
            super();
            this.outputFieldsDeclaration = outputFieldsDeclaration;
            this.statusCodeField = statusCodeField;
            this.subIndicatorField = subIndicatorField;
            this.dunsField = dunsField;
            this.ddunsField = ddunsField;
            this.gdunsField = gdunsField;
            this.hqDunsField = hqDunsField;
        }
    }
}
