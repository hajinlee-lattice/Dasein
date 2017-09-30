package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.metadata.transaction.NamedPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TransactionAggregator
        extends BaseAggregator<TransactionAggregator.Context>
        implements Aggregator<TransactionAggregator.Context> {

    private static final Logger log = LoggerFactory.getLogger(TransactionAggregator.class);

    private static final long serialVersionUID = 6298800516602499546L;

    private List<String> productIds;
    private List<String> periods;
    private List<String> metrics;
    private String idField;
    private String productField;
    private String dateField;
    private String quantityField;
    private String amountField;

    private List<String> uniquePeriods;
    private List<String[]> timeRanges;

    public static class Context extends BaseAggregator.Context {
        String accountId = null;
        HashMap<String, HashMap<String, Boolean>> purchasedTable = new HashMap<String, HashMap<String, Boolean>>();
        HashMap<String, HashMap<String, Long>> quantityTable = new HashMap<String, HashMap<String, Long>>();
        HashMap<String, HashMap<String, Double>> amountTable = new HashMap<String, HashMap<String, Double>>();
    }

    public TransactionAggregator(Fields fieldDeclaration, List<String> productIds, List<String> periods, List<String> metrics,
                                     String idField, String productField, String dateField, String quantityField, String amountField) {

        super(fieldDeclaration);
        this.productIds = productIds;
        this.periods = periods;
        this.metrics = metrics;
        this.idField = idField;
        this.productField = productField;
        this.dateField = dateField;
        this.quantityField = quantityField;
        this.amountField = amountField;

        this.uniquePeriods = new ArrayList<String>();
        this.timeRanges = new ArrayList<String[]>();
        for (String period : periods) {
            boolean found = false;
            for (String uniquePeriod : uniquePeriods) {
                if (period.equals(uniquePeriod)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                uniquePeriods.add(period);
                NamedPeriod namedPeriod = NamedPeriod.fromName(period);
                timeRanges.add(namedPeriod.getTimeRange());
            }
       }
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        boolean isDummy = false;
        Object grpObj = group.getObject(idField);
        if (grpObj == null) {
            isDummy = true;
        }
        if (grpObj instanceof Utf8) {
            isDummy = StringUtils.isBlank(grpObj.toString());
        }
        if (grpObj instanceof String) {
            isDummy = StringUtils.isBlank((String) grpObj);
        }
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();

        for (int i = 0; i < periods.size(); i++) {
            String period = periods.get(i);
            String metric = metrics.get(i);
            if (metric.equals(TransactionMetrics.PURCHASED.getName())) {
                    HashMap<String, Boolean> periodPurchased = new HashMap<String, Boolean>();
                    context.purchasedTable.put(period, periodPurchased);
            } else if (metric.equals(TransactionMetrics.QUANTITY.getName())) {
                    HashMap<String, Long> periodQuantity = new HashMap<String, Long>();
                    context.quantityTable.put(period, periodQuantity);
            } else if (metric.equals(TransactionMetrics.AMOUNT.getName())) {
                    HashMap<String, Double> periodAmount = new HashMap<String, Double>();
                    context.amountTable.put(period, periodAmount);
            }
        }

        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        if (context.accountId == null) {
            context.accountId = arguments.getString(idField);
        }
        String productId = arguments.getString(productField);
        String date = arguments.getString(dateField);
        Long quantity= arguments.getLong(quantityField);
        Double amount= arguments.getDouble(amountField);
        for (int i = 0; i < timeRanges.size(); i++) {
            String[] timeRange = timeRanges.get(i);
            if ((timeRange[0].compareTo(date) <= 0) && (timeRange[1].compareTo(date) >= 0)) {
                update(context, productId, uniquePeriods.get(i), quantity, amount);
            }
        }
        return context;
    }

    private void update(Context context, String productId, String period, Long quantity, Double amount) {

        Map<String, Boolean> periodPurchased = context.purchasedTable.get(period);
        if (periodPurchased != null) {
            periodPurchased.put(productId, Boolean.TRUE);
        }

        Map<String, Long> periodQuantity = context.quantityTable.get(period);
        if (periodQuantity != null) {
            if (quantity == null) {
                quantity = new Long(1);
            }
            Long curQuantity = periodQuantity.get(productId);
            periodQuantity.put(productId, ((curQuantity == null) ? quantity : (quantity + curQuantity)));
        }

        Map<String, Double> periodAmount = context.amountTable.get(period);
        if (periodAmount != null) {
            if (amount !=  null) {
                Double curAmount = periodAmount.get(productId);
                periodAmount.put(productId, ((curAmount == null) ? amount : (amount + curAmount)));
            }
        }
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        if (context.accountId == null) {
            return null;
        }
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(0, context.accountId);

        int loc = 1;
        for (int i = 0; i < periods.size(); i++) {
            String period = periods.get(i);
            String metric = metrics.get(i);
            if (metric.equals(TransactionMetrics.PURCHASED.getName())) {
                HashMap<String, Boolean> periodPurchased = context.purchasedTable.get(period);
                for (String productId : productIds) {
                    result.set(loc++, periodPurchased.get(productId));
                }
            } else if (metric.equals(TransactionMetrics.QUANTITY.getName())) {
                HashMap<String, Long> periodQuantity = context.quantityTable.get(period);
                for (String productId : productIds) {
                    result.set(loc++, periodQuantity.get(productId));
                }
            } else if (metric.equals(TransactionMetrics.AMOUNT.getName())) {
                HashMap<String, Double> periodAmount = context.amountTable.get(period);
                for (String productId : productIds) {
                    result.set(loc++, periodAmount.get(productId));
                }
            } else {
                for (String productId : productIds) {
                     result.set(loc++, null);
                }
            }
        }

        return result;
    }
}
