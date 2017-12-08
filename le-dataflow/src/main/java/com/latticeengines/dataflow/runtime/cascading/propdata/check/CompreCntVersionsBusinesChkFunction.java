package com.latticeengines.dataflow.runtime.cascading.propdata.check;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.CheckCode;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class CompreCntVersionsBusinesChkFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -2767865275111332041L;
    private static final Log log = LogFactory.getLog(CompreCntVersionsBusinesChkFunction.class);
    private String prevVersCountField;
    private String currVersCountField;
    private double threshold;

    public CompreCntVersionsBusinesChkFunction(String prevVersCountField, String currVersCountField, double threshold) {
        super(generateFieldDeclaration());
        this.prevVersCountField = prevVersCountField;
        this.currVersCountField = currVersCountField;
        this.threshold = threshold;
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
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        try {
            int prevVersionCount = Integer.parseInt(arguments.getObject(prevVersCountField).toString());
            int currVersionCount = Integer.parseInt(arguments.getObject(currVersCountField).toString());
            double avgOfVersions = (prevVersionCount + currVersionCount) / 2.0;
            int diffCount = (currVersionCount - prevVersionCount);
            double percentDiff = ((diffCount) / avgOfVersions) * 100;
            if (Math.abs(percentDiff) > threshold) {
                // check code
                result.set(0, CheckCode.ExceededVersionDiffForNumOfBusinesses.name());
                // check field
                result.set(3, currVersCountField);
                // check value
                result.set(4, String.format("%.2f", percentDiff));
                // check message
                result.set(5,
                        CheckCode.ExceededVersionDiffForNumOfBusinesses.getMessage(String.format("%.2f", percentDiff),
                                prevVersionCount, currVersionCount,
                                String.format("%.2f", (Math.abs(percentDiff) - threshold))));
                functionCall.getOutputCollector().add(result);
            }
        } catch (Exception e) {
            log.info("Exception raised due to : " + e);
        }
    }

}
