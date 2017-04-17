package com.latticeengines.dataflow.runtime.cascading.propdata;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * group by domain/checksum, output (domain/checksum, chosen_dedup_Id) 
 * randomly pick an id in the group: for the (i)th row in the group, use probability = 1/i to change current chosen id to id of (i)th row
 * it could be justified that every id could be chosen with same probability
 */
public class MatchDedupeIdAggregator //
        extends BaseAggregator<MatchDedupeIdAggregator.Context> //
        implements Aggregator<MatchDedupeIdAggregator.Context> {
    private static final long serialVersionUID = 1L;
    private static final Log log = LogFactory.getLog(MatchDedupeIdAggregator.class);

    private final String grpField;
    private final String tmpIdField;

    public static class Context extends BaseAggregator.Context
    {
        String chosenId = null;
        Integer seqInGroup = 0;
    }

    public MatchDedupeIdAggregator(String grpField, String tmpIdField) {
        super(new Fields(grpField, tmpIdField));
        this.grpField = grpField;
        this.tmpIdField = tmpIdField;
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
    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        context.seqInGroup++;
        if (Math.random() <= (1.0 / (double) context.seqInGroup)) {
            context.chosenId = arguments.getString(INT_LDC_DEDUPE_ID);
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        TupleEntry group = context.groupTuple;
        if (context.chosenId != null) {
            Tuple result = Tuple.size(2);
            result.set(namePositionMap.get(grpField), group.getObject(grpField));
            result.set(namePositionMap.get(tmpIdField), context.chosenId);
            return result;
        } else {
            return null;
        }
    }

}
