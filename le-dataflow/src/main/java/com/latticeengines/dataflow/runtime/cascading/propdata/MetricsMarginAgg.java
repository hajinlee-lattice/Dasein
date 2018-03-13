package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Margin on product A purchase by a given account = Round (100* ((Revenue -
 * Cost) of product A sell within last X weeks / cost of product A.)) 
 * If revenue or cost is not available, margin is not available
 */
public class MetricsMarginAgg extends BaseAggregator<MetricsMarginAgg.Context>
        implements Aggregator<MetricsMarginAgg.Context> {

    private static final long serialVersionUID = -2214807584804576474L;

    private String marginField;

    public static class Context extends BaseAggregator.Context {
        double totalAmount = 0;
        double totalCost = 0;
        String accountId;
        String productId;
    }

    public MetricsMarginAgg(Fields fieldDeclaration, String marginField) {
        super(fieldDeclaration);
        this.marginField = marginField;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.accountId = group.getString(InterfaceName.AccountId.name());
        context.productId = group.getString(InterfaceName.ProductId.name());
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        double amount = arguments.getDouble(InterfaceName.TotalAmount.name());    // treat null as 0
        double cost = arguments.getDouble(InterfaceName.TotalCost.name());    // treat null as 0
        context.totalAmount += amount;
        context.totalCost += cost;
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(namePositionMap.get(InterfaceName.AccountId.name()), context.accountId);
        result.set(namePositionMap.get(InterfaceName.ProductId.name()), context.productId);
        Integer margin = null;
        if (context.totalAmount != 0 && context.totalCost != 0) {
            margin = (int) Math.round(100.0 * (context.totalAmount - context.totalCost) / context.totalCost);
        }
        result.set(namePositionMap.get(marginField), margin);
        return result;
    }
}
