package com.latticeengines.datacloud.dataflow.utils;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.DuplicatedValueCheckAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.CheckCode;
import com.latticeengines.domain.exposed.datacloud.check.CheckParam;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public final class CheckUtils {

    public static Node runCheck(Node input, CheckParam param) {
        CheckCode checkCode = param.getCheckCode();
        switch (checkCode) {
            case DuplicatedValue:
                return checkDuplicatedValue(input, (DuplicatedValueCheckParam) param);
            default:
                throw new UnsupportedClassVersionError("Do not support check code " + checkCode);
        }
    }

    private static Node checkDuplicatedValue(Node input, DuplicatedValueCheckParam param) {
        List<FieldMetadata> fms = Arrays.asList(
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        String targetField = param.getGroupByFields().get(0);
        DuplicatedValueCheckAggregator aggregator = new DuplicatedValueCheckAggregator(targetField);
        return input.groupByAndAggregate(new FieldList(targetField), aggregator, fms);
    }

}
