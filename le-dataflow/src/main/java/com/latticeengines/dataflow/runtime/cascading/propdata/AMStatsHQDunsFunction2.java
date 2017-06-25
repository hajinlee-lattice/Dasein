package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Arrays;
import java.util.List;
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
public class AMStatsHQDunsFunction2 extends BaseOperation<Map> //
        implements Function<Map> {
    private static final long serialVersionUID = -4039806083023012431L;
    private static final Log log = LogFactory.getLog(AMStatsHQDunsFunction2.class);

    private String subIndicatorField;
    private String statusCodeField;
    private String dunsField;
    private String ddunsField;
    private String gdunsField;

    public AMStatsHQDunsFunction2(Params params) {
        super(new Fields(params.hqDunsField));
        subIndicatorField = params.subIndicatorField;
        statusCodeField = params.statusCodeField;
        dunsField = params.dunsField;
        ddunsField = params.ddunsField;
        gdunsField = params.gdunsField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Map> functionCall) {
        TupleEntry entry = functionCall.getArguments();

        String subIndicator = (String) entry.getObject(subIndicatorField);
        String statusCode = (String) entry.getObject(statusCodeField);

        if (subIndicator == null || statusCode == null) {
            throw new RuntimeException("subIndicator and statusCode both should not be null ");
        }

        String duns = (String) entry.getObject(dunsField);
        String dduns = (String) entry.getObject(ddunsField);
        String gduns = (String) entry.getObject(gdunsField);
        String hqDuns = null;

        try {
            hqDuns = calculateHQDuns(Integer.parseInt(subIndicator), //
                    Integer.parseInt(statusCode), //
                    duns, dduns, gduns);
        } catch (NumberFormatException e) {
            log.error("subIndicator = " + subIndicator + ", statusCode = " + statusCode
                    + " should both be valid numbers: " + e.getMessage());
        }

        functionCall.getOutputCollector().add(new Tuple(hqDuns));
    }

    static String calculateHQDuns(int subIndicator, int statusCode, //
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

    public static Params.Builder paramsBuilder() {
        return new Params.Builder();
    }

    public static class Params {
        String statusCodeField;
        String subIndicatorField;
        String dunsField;
        String ddunsField;
        String gdunsField;
        String hqDunsField;

        public Params(String statusCodeField, //
                String subIndicatorField, String dunsField, String ddunsField, //
                String gdunsField, String hqDunsField) {
            super();
            this.statusCodeField = statusCodeField;
            this.subIndicatorField = subIndicatorField;
            this.dunsField = dunsField;
            this.ddunsField = ddunsField;
            this.gdunsField = gdunsField;
            this.hqDunsField = hqDunsField;
        }

        public List<String> applyToFields() {
            return Arrays.asList(
                    statusCodeField, //
                    subIndicatorField, //
                    dunsField, //
                    ddunsField, //
                    gdunsField //
            );
        }

        public static class Builder {
            String statusCodeField;
            String subIndicatorField;
            String dunsField;
            String ddunsField;
            String gdunsField;
            String hqDunsField;

            public Builder statusCodeField(String statusCodeField) {
                this.statusCodeField = statusCodeField;
                return this;
            }

            public Builder subIndicatorField(String subIndicatorField) {
                this.subIndicatorField = subIndicatorField;
                return this;
            }

            public Builder dunsField(String dunsField) {
                this.dunsField = dunsField;
                return this;
            }

            public Builder ddunsField(String ddunsField) {
                this.ddunsField = ddunsField;
                return this;
            }

            public Builder gdunsField(String gdunsField) {
                this.gdunsField = gdunsField;
                return this;
            }

            public Builder hqDunsField(String hqDunsField) {
                this.hqDunsField = hqDunsField;
                return this;
            }

            public Params build() {
                return new Params(statusCodeField, subIndicatorField, dunsField, ddunsField, gdunsField, hqDunsField);
            }

        }
    }
}
