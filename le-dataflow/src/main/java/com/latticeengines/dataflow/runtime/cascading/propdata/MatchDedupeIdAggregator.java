package com.latticeengines.dataflow.runtime.cascading.propdata;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_POPULATED_ATTRS;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * group by domain/checksum, output (domain/checksum, highest_pop_dedupe_id)
 */
public class MatchDedupeIdAggregator //
        extends BaseAggregator<MatchDedupeIdAggregator.Context> //
        implements Aggregator<MatchDedupeIdAggregator.Context> {
    private static final long serialVersionUID = 1L;

    public static class Context extends BaseAggregator.Context
    {
        String highestPopId = null;
        Integer highestPopulation = -1;
    }

    public MatchDedupeIdAggregator(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(0);
        if (grpObj == null) {
            return true;
        }
        if (grpObj instanceof Utf8) {
            return StringUtils.isBlank(grpObj.toString());
        }
        if (grpObj instanceof String) {
            return StringUtils.isBlank((String) grpObj);
        }
        return true;
    }

    @Override
    protected Context initializeContext() {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        Object obj = arguments.getObject(INT_LDC_POPULATED_ATTRS);
        if (obj != null) {
            Integer population = arguments.getInteger(INT_LDC_POPULATED_ATTRS);
            if (population > context.highestPopulation) {
                context.highestPopulation = population;
                context.highestPopId = arguments.getString(INT_LDC_DEDUPE_ID);
            }
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        TupleEntry group = context.groupTuple;
        if (context.highestPopId != null) {
            Tuple result = Tuple.size(2);
            result.set(0, group.getObject(0));
            result.set(1, context.highestPopId);
            return result;
        } else {
            return null;
        }
    }

}
