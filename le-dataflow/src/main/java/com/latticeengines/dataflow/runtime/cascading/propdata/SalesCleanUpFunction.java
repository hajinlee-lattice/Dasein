package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class SalesCleanUpFunction extends CleanupFunction implements Function {

    private static final Log log = LogFactory.getLog(SalesCleanUpFunction.class);

    private static final long serialVersionUID = -3909325381537456515L;
    private String sales;
    private static final BigInteger oneBillion = new BigInteger("1000000000");

    public SalesCleanUpFunction(String sales) {
        super(new Fields(sales), true);
        this.sales = sales;
    }

    @Override
    protected Tuple cleanupArguments(TupleEntry arguments) {
        try {
            String sales = arguments.getString(this.sales);
            String result = "";
            if (sales.contains("$")) {
                result = sales.substring(1);
            } else {
                result = sales;
            }
            Double resultValue = Double.parseDouble(result);
            BigDecimal bd = new BigDecimal(oneBillion);
            BigDecimal finalResult = bd.multiply(BigDecimal.valueOf(resultValue));
            if (finalResult.doubleValue() == 0.00) {
                // for salesValue = 0, need to use dnb value
                Long longValue = null;
                return new Tuple(longValue);
            } else {
                return new Tuple(finalResult.longValue());
            }
        } catch (Exception e) {
            log.error("Error in cleaning up arguments");
            return null;
        }
    }
}
