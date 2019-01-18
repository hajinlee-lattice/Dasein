package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class OrbSecSrcSelectPriDomAggregator extends BaseAggregator<OrbSecSrcSelectPriDomAggregator.Context>
        implements Aggregator<OrbSecSrcSelectPriDomAggregator.Context> {

    private static final long serialVersionUID = 3454191979161403133L;
    private String alexaRankField;
    private String orbPriDomainField;
    private String orbSecDomainField;

    public OrbSecSrcSelectPriDomAggregator(Fields fieldDeclaration, String orbPriDomainField, String orbSecDomainField,
            String alexaRankField) {
        super(fieldDeclaration);
        this.orbPriDomainField = orbPriDomainField;
        this.alexaRankField = alexaRankField;
        this.orbSecDomainField = orbSecDomainField;
    }

    public static class Context extends BaseAggregator.Context {
        String orbPriDomain = null;
        String orbSecDomain = null;
        int alexaRank = 0;
        Tuple result;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(orbSecDomainField);
        if (grpObj == null) {
            return true;
        }
        if (grpObj instanceof Utf8) {
            return StringUtils.isBlank(grpObj.toString());
        }
        if (grpObj instanceof String) {
            return StringUtils.isBlank((String) grpObj);
        }
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.orbSecDomain = group.getString(orbSecDomainField);
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        int alexaRankVal = arguments.getInteger(alexaRankField);
        String orbPriDomain = arguments.getString(orbPriDomainField);
        String orbSecDomain = arguments.getString(orbSecDomainField);
        if (context.orbPriDomain == null || alexaRankVal > context.alexaRank) {
            context.orbPriDomain = orbPriDomain;
        }
        if (context.orbSecDomain == null) {
            context.orbSecDomain = orbSecDomain;
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        context.result = new Tuple();
        context.result = Tuple.size(getFieldDeclaration().size());
        context.result.set(0, context.orbPriDomain);
        context.result.set(1, context.orbSecDomain);
        return context.result;
    }
}
