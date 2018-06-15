package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMSeedPriDomainAggregator extends BaseAggregator<AMSeedPriDomainAggregator.Context>
        implements Aggregator<AMSeedPriDomainAggregator.Context> {

    private List<String> groupFields;

    public AMSeedPriDomainAggregator(Fields fieldDeclaration, List<String> groupFields) {
        super(fieldDeclaration);
        this.groupFields = groupFields;
    }

    public static class Context extends BaseAggregator.Context {
        Long id = null;
        String isPriDom = null;
        String isPriAct = null;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        for (String groupField : groupFields) {
            Object grpObj = group.getObject(groupField);
            if (grpObj == null) {
                return true;
            }
            if (grpObj instanceof Utf8 && StringUtils.isBlank(grpObj.toString())) {
                return true;
            }
            if (grpObj instanceof String && StringUtils.isBlank((String) grpObj)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        if (context.id == null) {
            return update(context, arguments);
        }
        int res = RuleBasedComparator.preferBooleanValuedStringAsTrue(
                arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT), context.isPriAct);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferBooleanValuedStringAsTrue(
                arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN), context.isPriDom);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        if (context.id != null) {
            return new Tuple(context.id);
        } else {
            return null;
        }
    }

    private Context update(Context context, TupleEntry arguments) {
        context.id = (Long) arguments.getObject(DataCloudConstants.LATTIC_ID);
        context.isPriDom = arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN);
        context.isPriAct = arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT);
        return context;
    }
}
