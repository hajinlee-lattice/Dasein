package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class SalesCleanUpFunction extends CleanupFunction implements Function {

    private static final Logger log = LoggerFactory.getLogger(SalesCleanUpFunction.class);
    private static final Double billionValue = 1000000000.0;

    private static final long serialVersionUID = -3909325381537456515L;
    private String totalSalesField;

    public SalesCleanUpFunction(String totalSalesField) {
        super(new Fields(totalSalesField), false);
        this.totalSalesField = totalSalesField;
    }

    @Override
    protected Tuple cleanupArguments(TupleEntry arguments) {
        try {
            String sales = arguments.getString(this.totalSalesField);
            String result = sales.replaceAll("[^\\d.]", "");
            if (StringUtils.isBlank(result)) {
                return null;
            }
            Double resultValue = Double.parseDouble(result);
            Double finalResult = billionValue * resultValue;
            if (finalResult.doubleValue() == 0.00) {
                // for salesValue = 0, need to use dnb value
                return null;
            } else {
                return new Tuple(finalResult.longValue());
            }
        } catch (Exception e) {
            log.error("Error in cleaning up arguments", e);
            return null;
        }
    }
}
