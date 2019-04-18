package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ManualDomainEnrichAggregator
        extends BaseAggregator<ManualDomainEnrichAggregator.Context>
        implements Aggregator<ManualDomainEnrichAggregator.Context> {
    private static final long serialVersionUID = 1059291855974074063L;
    private static final String MANUAL_DOMAIN_SOURCE = "Manual";
    private String manSeedDomain;
    private String amSeedDomain;
    private int domainLoc;
    private int domainSourceLoc;
    private String isPrimaryDomain;

    public ManualDomainEnrichAggregator(Fields fieldDeclaration, String manSeedDomain,
            String amSeedDomain, String isPrimaryDomain, String domainSource) {
        super(fieldDeclaration);
        this.manSeedDomain = manSeedDomain;
        this.amSeedDomain = amSeedDomain;
        this.isPrimaryDomain = isPrimaryDomain;
        this.domainLoc = namePositionMap.get(amSeedDomain);
        this.domainSourceLoc = namePositionMap.get(domainSource);
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
        if (context.result == null || ("Y").equals(arguments.getString(isPrimaryDomain))) {
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
        // overwrite value based on isPrimaryDomain
        for (int i = 0; i < arguments.size(); i++) {
            context.result.set(i, arguments.getObject(i));
        }
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        boolean checkFlag = false;
        for (String obj : context.manSeedDomainSet) {
            if (!(context.amSeedDomainSet).contains(obj)) {
                // setting flag to indicate new domain was found
                checkFlag = true;
                context.result.set(domainLoc, obj);
                context.result.set(domainSourceLoc, MANUAL_DOMAIN_SOURCE);
            }
        }
        if (checkFlag == false) {
            context.result = null;
        }
        return context.result;
    }

    public static class Context extends BaseAggregator.Context {
        Set<String> manSeedDomainSet = new HashSet<String>();
        Set<String> amSeedDomainSet = new HashSet<String>();
        Tuple result;
    }
}
