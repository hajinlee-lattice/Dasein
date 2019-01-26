package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class OrbSecSrcSelectPriDomAggregator extends BaseAggregator<OrbSecSrcSelectPriDomAggregator.Context>
        implements Aggregator<OrbSecSrcSelectPriDomAggregator.Context> {

    private static final long serialVersionUID = 3454191979161403133L;
    private String alexaRankField;

    public OrbSecSrcSelectPriDomAggregator(Fields fieldDeclaration, String alexaRankField) {
        super(fieldDeclaration);
        this.alexaRankField = alexaRankField;
    }

    public static class Context extends BaseAggregator.Context {
        String orbPriDomain = null;
        String orbSecDomain = null;
        Integer alexaRank = null;
        Tuple result;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(DataCloudConstants.ORBSEC_ATTR_SECDOM);
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
        context.orbSecDomain = group.getString(DataCloudConstants.ORBSEC_ATTR_SECDOM);
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        Integer alexaRankVal = (Integer) arguments.getObject(alexaRankField);
        String orbPriDomain = arguments.getString(DataCloudConstants.ORBSEC_ATTR_PRIDOM);
        if (context.orbPriDomain == null
                || (alexaRankVal != null && context.alexaRank == null)
                || (alexaRankVal != null && context.alexaRank != null
                        && alexaRankVal.intValue() < context.alexaRank.intValue())) {
            context.orbPriDomain = orbPriDomain;
            if (alexaRankVal != null) {
                context.alexaRank = alexaRankVal;
            }
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
