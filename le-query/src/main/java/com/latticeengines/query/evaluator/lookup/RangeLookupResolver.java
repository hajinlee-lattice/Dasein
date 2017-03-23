package com.latticeengines.query.evaluator.lookup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class RangeLookupResolver extends LookupResolver {
    private RangeLookup lookup;
    private Lookup secondaryLookup;

    public RangeLookupResolver(RangeLookup lookup, SchemaInterpretation rootObjectType, DataCollection dataCollection,
            Lookup secondaryLookup) {
        super(dataCollection, rootObjectType);
        this.lookup = lookup;
        this.secondaryLookup = secondaryLookup;
    }

    @Override
    public List<ComparableExpression<String>> resolve() {
        ColumnLookup columnLookup = (ColumnLookup) secondaryLookup;

        if (lookup.getRange() == null) {
            throw new LedpException(LedpCode.LEDP_37000);
        }

        if (columnLookup != null) {
            Attribute attribute = getAttribute(columnLookup);

            List<BucketRange> buckets = attribute.getBucketRangeList();
            if (buckets != null && buckets.size() > 0) {
                int bucketIdx = 0;

                while (bucketIdx < buckets.size()) {
                    BucketRange bucket = buckets.get(bucketIdx);

                    if (lookup.getRange().equals(bucket)) {
                        break;
                    }
                    bucketIdx++;
                }

                if (bucketIdx == buckets.size()) {
                    throw new LedpException(LedpCode.LEDP_37001, new String[] { attribute.getName(),
                            lookup.getRange().toString() });
                }

                return Collections.singletonList(Expressions.asComparable(Integer.toString(bucketIdx)));
            }
        }

        List<ComparableExpression<String>> expressions = new ArrayList<>();
        ComparableExpression<String> min = Expressions.asComparable(lookup.getRange().getMin().toString());
        ComparableExpression<String> max = Expressions.asComparable(lookup.getRange().getMax().toString());
        expressions.add(min);
        expressions.add(max);
        return expressions;
    }
}
