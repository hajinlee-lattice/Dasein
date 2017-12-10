package com.latticeengines.datacloud.dataflow.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.CheckFieldNotEmptyFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.CheckFieldPopulationThresholdFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.CompreCntVersionsBusinesChkFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.CompreCntVersionsrDomOnlyChkFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.DuplicatedValueCheckAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.ExceededCountCheckFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.IncompleteCoverageColCheckAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.IncompleteCoverageRowCheckFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.CheckCode;
import com.latticeengines.domain.exposed.datacloud.check.CheckParam;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValuesWithStatusCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.EmptyFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceedCntDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceedDomDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceededCountCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.IncompleteCoverageForColCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.IncompleteCoverageForRowCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.UnderPopulatedFieldCheckParam;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.microsoft.sqlserver.jdbc.StringUtils;

public final class CheckUtils {

    private final static String STATUS_ACTIVE = "ACTIVE";
    private final static String TOTAL_COUNT = "__COUNT__";
    private final static String PREV_TOTAL_COUNT = "__PREV_COUNT__";
    private final static String POPULATED_COUNT = "__POPULATED_COUNT__";
    private final static String DUMMY_GROUP = "DUMMY_GROUP";
    private final static String DUMMY_VALUE = "DUMMY_VALUE";
    protected Map<String, Integer> namePositionMap;

    public static Node runCheck(List<Node> input, CheckParam param) {
        CheckCode checkCode = param.getCheckCode();
        switch (checkCode) {
            case DuplicatedValue:
                return checkDuplicatedValue(input.get(0), (DuplicatedValueCheckParam) param);
            case ExceededCount:
                return checkExceededCount(input.get(0), (ExceededCountCheckParam) param);
            case EmptyField:
                return checkEmptyField(input.get(0), (EmptyFieldCheckParam) param);
            case UnderPopulatedField:
                return checkUnderPopulatedField(input.get(0), (UnderPopulatedFieldCheckParam) param);
            case OutOfCoverageValForCol:
                return checkOutOfCoverageValForCol(input.get(0), (IncompleteCoverageForColCheckParam) param);
            case OutOfCoverageValForRow:
                return checkOutOfCoverageValForRow(input.get(0), (IncompleteCoverageForRowCheckParam) param);
            case DuplicatedValuesWithStatus:
                return checkDuplicatedValueWithStatus(input.get(0), (DuplicatedValuesWithStatusCheckParam) param);
            case ExceededVersionDiffForDomOnly:
                return checkDiffVersionDomOnlyCount(input, (ExceedDomDiffBetwenVersionChkParam) param);
            case ExceededVersionDiffForNumOfBusinesses:
                return checkDiffVersionBusinesses(input, (ExceedCntDiffBetwenVersionChkParam) param);
            default:
                throw new UnsupportedClassVersionError("Do not support check code " + checkCode);
        }
    }

    private static List<FieldMetadata> fieldMetadataPrep(Node input, CheckParam param) {
        return Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
    }

    private static Node checkDuplicatedValue(Node input, DuplicatedValueCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        Object targetField = param.getGroupByFields().get(0);
        DuplicatedValueCheckAggregator aggregator = new DuplicatedValueCheckAggregator(targetField);
        return input.groupByAndAggregate(new FieldList((String) targetField), aggregator, fms);
    }

    private static Node checkDuplicatedValueWithStatus(Node input, DuplicatedValuesWithStatusCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        Object statusField = param.getStatus();
        Object targetField = param.getGroupByFields().get(0);
        // filtering records with status = "ACTIVE"
        String checkStatusExpression = String.format("%s.equals(\"%s\")", statusField, STATUS_ACTIVE);
        Node resultNode = input //
                .filter(checkStatusExpression, new FieldList(input.getFieldNames())) //
                .retain(new FieldList((String) param.getKeyField(), (String) targetField));
        // checking duplicates check for the filtered records
        DuplicatedValueCheckAggregator aggregator = new DuplicatedValueCheckAggregator(targetField);
        return resultNode.groupByAndAggregate(new FieldList((String) targetField), aggregator, fms);
    }

    private static Node checkExceededCount(Node input, ExceededCountCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        boolean lessThanCntFlag = param.getCntLessThanThresholdFlag();
        long threshold = param.getExceedCountThreshold();
        Node inputNodeCount = input.count(TOTAL_COUNT);
        inputNodeCount = inputNodeCount.apply(new ExceededCountCheckFunction(TOTAL_COUNT, threshold, lessThanCntFlag),
                new FieldList(TOTAL_COUNT), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));

        return inputNodeCount;
    }

    private static Node checkEmptyField(Node input, EmptyFieldCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        Object keyField = param.getKeyField();
        Object checkNullField = param.getCheckEmptyField();
        Node nullFieldCheck = input.apply(new CheckFieldNotEmptyFunction(checkNullField, keyField),
                new FieldList((String) checkNullField, (String) keyField), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return nullFieldCheck;
    }

    private static Node checkUnderPopulatedField(Node input, UnderPopulatedFieldCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        Object targetField = param.getGroupByFields().get(0);
        double threshold = param.getThreshold();
        Node inputTotalCount = input.count(TOTAL_COUNT) //
                .renamePipe("TotalCount");
        if (StringUtils.isEmpty("")) {

        }
        // filtering required field to check having empty/null value
        String checkStatusExpression = String.format("%s != null && %s !=\"\"", targetField, targetField);
        Node resultNode = input //
                .filter(checkStatusExpression, new FieldList(input.getFieldNames()));
        Node populatedTotalCount = resultNode.count(POPULATED_COUNT) //
                .renamePipe("PopulatedCount");
        Node combinedResult = inputTotalCount //
                .combine(populatedTotalCount);
        combinedResult = combinedResult.apply(
                new CheckFieldPopulationThresholdFunction(TOTAL_COUNT, POPULATED_COUNT, threshold, targetField),
                new FieldList(TOTAL_COUNT, POPULATED_COUNT), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return combinedResult;
    }

    private static Node checkOutOfCoverageValForCol(Node input, IncompleteCoverageForColCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        Object targetField = param.getGroupByFields().get(0);
        List<Object> expectedValues = param.getExpectedFieldValues();
        Node groupedInput = input //
                .groupByAndLimit(new FieldList((String) targetField), 1) //
                .addColumnWithFixedValue(DUMMY_GROUP, DUMMY_VALUE, String.class);
        IncompleteCoverageColCheckAggregator aggregator = new IncompleteCoverageColCheckAggregator(targetField,
                expectedValues);
        return groupedInput.groupByAndAggregate(new FieldList(DUMMY_GROUP), aggregator, fms);
    }

    private static Node checkOutOfCoverageValForRow(Node input, IncompleteCoverageForRowCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        Object keyField = param.getKeyField();
        Object targetField = param.getGroupByFields().get(0);
        List<Object> expectedFieldValues = param.getExpectedFieldValues();
        Node outOfCoverageRows = input.apply(
                new IncompleteCoverageRowCheckFunction(targetField, expectedFieldValues, keyField),
                new FieldList((String) targetField, (String) keyField), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return outOfCoverageRows;
    }

    private static Node checkDiffVersionDomOnlyCount(List<Node> input, ExceedDomDiffBetwenVersionChkParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input.get(0), param);
        Object prevNotNullField = param.getPrevVersionNotEmptyField();
        Object currNotNullField = param.getCurrVersionNotEmptyField();
        Object prevNullField = param.getPrevVersionEmptyField();
        Object currNullField = param.getCurrVersionEmptyField();
        String checkPrevVersionDomainOnly = String.format(
                "(%s != \"\") && (%s != null) && ((%s == null) || (%s == \"\"))", prevNotNullField, prevNotNullField,
                prevNullField, prevNullField);
        String checkCurrVersionDomainOnly = String.format(
                "(%s != \"\") && (%s != null) && ((%s == null) || (%s == \"\"))", currNotNullField, currNotNullField,
                currNullField, currNullField);
        Node filteredPrevVersion = input.get(0) //
                .filter(checkPrevVersionDomainOnly, new FieldList(input.get(0).getFieldNames())) //
                .renamePipe("filteredPrevVersion");
        Node filteredCurrVersion = input.get(1) //
                .filter(checkCurrVersionDomainOnly, new FieldList(input.get(1).getFieldNames())) //
                .renamePipe("filteredCurrVersion");
        Node prevVersionCount = filteredPrevVersion.count(PREV_TOTAL_COUNT) //
                .retain(new FieldList(PREV_TOTAL_COUNT));
        Node currVersionCount = filteredCurrVersion.count(TOTAL_COUNT) //
                .retain(new FieldList(TOTAL_COUNT));
        Node combinedCounts = prevVersionCount //
                .combine(currVersionCount);
        Node exceededDomOnly = combinedCounts.apply(
                new CompreCntVersionsrDomOnlyChkFunction(PREV_TOTAL_COUNT, TOTAL_COUNT),
                new FieldList(PREV_TOTAL_COUNT, TOTAL_COUNT), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return exceededDomOnly;
    }

    private static Node checkDiffVersionBusinesses(List<Node> input,
            ExceedCntDiffBetwenVersionChkParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input.get(0), param);
        double threshold = param.getThreshold();
        Node prevVersionCount = input.get(0).count(PREV_TOTAL_COUNT) //
                .retain(new FieldList(PREV_TOTAL_COUNT));
        Node currVersionCount = input.get(1).count(TOTAL_COUNT) //
                .retain(new FieldList(TOTAL_COUNT));
        Node combinedCounts = prevVersionCount //
                .combine(currVersionCount);
        Node exceededBusinesses = combinedCounts.apply(
                new CompreCntVersionsBusinesChkFunction(PREV_TOTAL_COUNT, TOTAL_COUNT, threshold),
                new FieldList(PREV_TOTAL_COUNT, TOTAL_COUNT), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return exceededBusinesses;
    }

}
