package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AMSeedDuDomAggregator extends BaseAggregator<AMSeedDuDomAggregator.Context>
        implements Aggregator<AMSeedDuDomAggregator.Context> {

    private static final Log log = LogFactory.getLog(AMSeedPriDomAggregator.class);

    private static final long serialVersionUID = 7153412402674903342L;

    private String dunsField;
    private String duDunsField;
    private String domField;
    private int duDunsLoc;
    private int duDomsLoc;


    public static class Context extends BaseAggregator.Context {
        String duDuns;
        StringBuilder duDomsBuilder = new StringBuilder();
    }

    public AMSeedDuDomAggregator(Fields fieldDeclaration, String dunsField, String duDunsField, String domField,
            String duDomsField) {
        super(fieldDeclaration);
        this.dunsField = dunsField;
        this.duDunsField = duDunsField;
        this.domField = domField;
        this.duDunsLoc = namePositionMap.get(duDunsField);
        this.duDomsLoc = namePositionMap.get(duDomsField);
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(duDunsField);
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
        Context context = new Context();
        Object grpObj = group.getObject(duDunsField);
        if (grpObj == null) {
            return context;
        }
        if (grpObj instanceof Utf8) {
            context.duDuns = grpObj.toString();
        } else if (grpObj instanceof String) {
            context.duDuns = (String) grpObj;
        }
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        String duDuns = arguments.getString(duDunsField);
        String duns = arguments.getString(dunsField);
        String domain = arguments.getString(domField);
        if (StringUtils.isNotBlank(duDuns) && StringUtils.isNotBlank(domain) && duDuns.equals(duns)) {
            context.duDomsBuilder.append(domain + "||");
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        if (context.duDomsBuilder.length() > 0) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(duDunsLoc, context.duDuns);
            result.set(duDomsLoc, context.duDomsBuilder.substring(0, context.duDomsBuilder.length() - 2));
            log.info("DuDuns: " + context.duDuns + " DuDoms: " + context.duDomsBuilder.toString());
            return result;
        } else {
            return null;
        }
    }
}
