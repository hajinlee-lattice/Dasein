package com.latticeengines.dataflow.runtime.cascading.propdata;

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
    private String dunsField;
    private String rootTypeField;

    public DomainTreeCountAggregator(Fields fieldDeclaration, String groupbyField, String domainField, String dunsField,
            String rootTypeField) {
        super(fieldDeclaration);
        this.groupbyField = groupbyField;
        this.domainField = domainField;
        this.dunsField = dunsField;
        this.rootTypeField = rootTypeField;
    }

    public static class Context extends BaseAggregator.Context {
        int count = 0;
        Object domain = null;
        Object duns = null;
        Object rootType = null;
        Tuple result;
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
        context.duns = arguments.getString(dunsField);
        context.rootType = arguments.getString(rootTypeField);
        context.count += 1;
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        context.result = new Tuple();
        context.result = Tuple.size(4);
        context.result.set(0, context.domain);
        context.result.set(1, context.duns);
        context.result.set(2, context.rootType);
        context.result.set(3, context.count);
        return context.result;
    }
}
