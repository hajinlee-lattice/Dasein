package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ExceededCountCheckAggregator extends BaseAggregator<ExceededCountCheckAggregator.Context>
        implements Aggregator<ExceededCountCheckAggregator.Context> {
    private static final long serialVersionUID = 2131883345415322975L;
    private String checkField;
    private Long expectedCount;

    public ExceededCountCheckAggregator(String keyField, Long expectedCount) {
        super(generateFieldDeclaration());
        this.checkField = keyField;
        this.expectedCount = expectedCount;
    }

    public static class Context extends BaseAggregator.Context {
        long count = 0;
    }

    private static Fields generateFieldDeclaration() {
        return new Fields( //
                DataCloudConstants.CHK_ATTR_CHK_CODE, //
                DataCloudConstants.CHK_ATTR_ROW_ID, //
                DataCloudConstants.CHK_ATTR_GROUP_ID, //
                DataCloudConstants.CHK_ATTR_CHK_FIELD, //
                DataCloudConstants.CHK_ATTR_CHK_VALUE, //
                DataCloudConstants.CHK_ATTR_CHK_MSG);
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(checkField);
        return grpObj == null;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        return null;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        // TODO Auto-generated method stub
        return null;
    }

}
