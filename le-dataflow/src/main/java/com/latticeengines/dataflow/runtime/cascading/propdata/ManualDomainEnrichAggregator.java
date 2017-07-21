package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ManualDomainEnrichAggregator extends BaseAggregator<ManualDomainEnrichAggregator.Context>
        implements Aggregator<ManualDomainEnrichAggregator.Context> {
    private static final long serialVersionUID = 1059291855974074063L;
    private String manSeedDomain;
    private String amSeedDomain;
    private int domainLoc;
    private String lePrimaryDomain;

    public static class Context extends BaseAggregator.Context {
        Set<String> manSeedDomainSet = new HashSet<String>();
        Set<String> amSeedDomainSet = new HashSet<String>();
        TupleEntry arguments;
        Tuple result;
    }

    public ManualDomainEnrichAggregator(Fields fieldDeclaration, String manSeedDomain,
            String amSeedDomain, String lePrimaryDomain) {
        super(fieldDeclaration);
        this.manSeedDomain = manSeedDomain;
        this.amSeedDomain = amSeedDomain;
        this.lePrimaryDomain = lePrimaryDomain;
        this.domainLoc = namePositionMap.get(amSeedDomain);
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        context.arguments = arguments;
        if (arguments.getString(lePrimaryDomain).equals("Y")) {
            setupTupleForArgument(context, arguments);
        }
        String manDomainVal = arguments.getString(manSeedDomain);
        String amDomainVal = arguments.getString(amSeedDomain);
        if (manDomainVal != null) {
            context.manSeedDomainSet.add(manDomainVal);
        }
        if (amDomainVal != null) {
            context.amSeedDomainSet.add(amDomainVal);
        }
        return context;
    }

    private void setupTupleForArgument(Context context, TupleEntry arguments) {
        context.result = new Tuple();
        context.result = Tuple.size(getFieldDeclaration().size());
        // overwrite value based on lePrimaryDomain
        for (int i = 0; i < arguments.size(); i++) {
            context.result.set(i, arguments.getObject(i));
        }
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        boolean checkFlag = false;
        if(!context.manSeedDomainSet.isEmpty()) {
            for (String obj : context.manSeedDomainSet) {
                if (obj != null && !(context.amSeedDomainSet).contains(obj)) {
                    // setting flag to indicate new domain was found
                    checkFlag = true;
                    // overwriting domain with remaining values from am seed as
                    // it is based on le primary domain
                    if (context.result == null) {
                        setupTupleForArgument(context, context.arguments);
                    }
                    context.result.set(domainLoc, obj);
                }
            }
            if (checkFlag == false) {
                context.result = null;
            }
            return context.result;
        } else {
            return null;
        }
    }
}
