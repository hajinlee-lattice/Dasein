package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DomainTreeCountAggregator extends BaseAggregator<DomainTreeCountAggregator.Context>
        implements Aggregator<DomainTreeCountAggregator.Context> {

    private static final long serialVersionUID = -4258093110031791835L;
    private String groupbyField;
    private String domainField;
    private String rootDunsField;
    private String dunsTypeField;

    public DomainTreeCountAggregator(Fields fieldDeclaration, String groupbyField, String domainField,
            String rootDunsField, String dunsTypeField) {
        super(fieldDeclaration);
        this.groupbyField = groupbyField;
        this.domainField = domainField;
        this.rootDunsField = rootDunsField;
        this.dunsTypeField = dunsTypeField;
    }

    public static class Context extends BaseAggregator.Context {
        int count = 0;
        String domain = null;
        String rootDuns = null;
        String dunsType = null;
        Tuple result;
        HashSet<String> rootDunsVisited = new HashSet<String>();
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(groupbyField);
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
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        context.domain = arguments.getString(domainField);
        context.dunsType = arguments.getString(dunsTypeField);
        context.rootDuns = arguments.getString(rootDunsField);
        if (context.rootDuns != null && !(context.rootDunsVisited.contains(context.rootDuns))) {
            context.count += 1;
            context.rootDunsVisited.add(context.rootDuns);
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        context.result = new Tuple();
        context.result = Tuple.size(4);
        context.result.set(0, context.domain);
        context.result.set(1, context.rootDuns);
        context.result.set(2, context.dunsType);
        context.result.set(3, context.count);
        return context.result;
    }
}
