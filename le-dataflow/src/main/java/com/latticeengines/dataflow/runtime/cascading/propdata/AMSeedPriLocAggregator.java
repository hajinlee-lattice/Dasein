package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMSeedPriLocAggregator extends BaseAggregator<AMSeedPriLocAggregator.Context>
        implements Aggregator<AMSeedPriLocAggregator.Context> {

    private static final long serialVersionUID = 6246503522063890526L;

    private List<String> groupFields;

    public static class Context extends BaseAggregator.Context {
        Long id = null;
        String duns = null;
        String duDuns = null;
        String guDuns = null;
        Integer employee = null;
        Long salesVol = null;
        String isPriLoc = null;
        String isPriAct = null;
        String country = null;
    }

    public AMSeedPriLocAggregator(Fields fieldDeclaration, List<String> groupFields) {
        super(fieldDeclaration);
        this.groupFields = groupFields;
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
                arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT),
                context.isPriAct);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferNotEmptyString(arguments.getString(DataCloudConstants.AMS_ATTR_DUNS),
                context.duns);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferNotEmptyString(arguments.getString(DataCloudConstants.ATTR_DU_DUNS),
                context.duDuns);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferLargerLongWithThreshold(
                (Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US),
                context.salesVol, 100000000, 10000000);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferEqualStrings(arguments.getString(DataCloudConstants.AMS_ATTR_DUNS),
                arguments.getString(DataCloudConstants.ATTR_DU_DUNS), context.duns,
                context.duDuns);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferEqualStrings(arguments.getString(DataCloudConstants.AMS_ATTR_DUNS),
                arguments.getString(DataCloudConstants.ATTR_GU_DUNS), context.duns,
                context.guDuns);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferLargerLong((Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US),
                context.salesVol);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferBooleanValuedStringAsTrue(
                arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION),
                context.isPriLoc);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferExpectedString(arguments.getString(DataCloudConstants.AMS_ATTR_COUNTRY),
                context.country,
                LocationUtils.USA);
        if (res > 0) {
            return update(context, arguments);
        } else if (res < 0) {
            return context;
        }
        res = RuleBasedComparator.preferLargerInteger(
                (Integer) arguments.getObject(DataCloudConstants.ATTR_EMPLOYEE_HERE),
                context.employee);
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
        context.duns = arguments.getString(DataCloudConstants.AMS_ATTR_DUNS);
        context.duDuns = arguments.getString(DataCloudConstants.ATTR_DU_DUNS);
        context.guDuns = arguments.getString(DataCloudConstants.ATTR_GU_DUNS);
        context.employee = (Integer) arguments.getObject(DataCloudConstants.ATTR_EMPLOYEE_HERE);
        context.salesVol = (Long) arguments.getObject(DataCloudConstants.ATTR_SALES_VOL_US);
        context.isPriLoc = arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION);
        context.isPriAct = arguments.getString(DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT);
        context.country = arguments.getString(DataCloudConstants.AMS_ATTR_COUNTRY);
        return context;
    }
}