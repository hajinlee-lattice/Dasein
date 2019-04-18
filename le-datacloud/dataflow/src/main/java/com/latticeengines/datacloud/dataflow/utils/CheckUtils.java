package com.latticeengines.datacloud.dataflow.utils;

import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.datacloud.check.EmptyFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceedCntDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceedDomDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceededCountCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.IncompleteCoverageForColChkParam;
import com.latticeengines.domain.exposed.datacloud.check.OutOfCoverageForRowChkParam;
import com.latticeengines.domain.exposed.datacloud.check.UnderPopulatedFieldCheckParam;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public final class CheckUtils {
    private static final String TOTAL_COUNT = "__COUNT__";
    private static final String PREV_TOTAL_COUNT = "__PREV_COUNT__";
    private static final String POPULATED_COUNT = "__POPULATED_COUNT__";
    private static final String DUMMY_GROUP = "DUMMY_GROUP";
    private static final String DUMMY_VALUE = "DUMMY_VALUE";
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
            case IncompleteCoverageForCol:
                return checkIncompleteCoverageForCol(input.get(0), (IncompleteCoverageForColChkParam) param);
            case OutOfCoverageValForRow:
                return checkOutOfCoverageValForRow(input.get(0), (OutOfCoverageForRowChkParam) param);
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
        List<String> targetFields = param.getGroupByFields();
        Boolean dupValWithStatusFlag = param.getCheckDupWithStatus();

        // filter null values for fields to check duplicates for
        String checkNullExpression = null;
        if (targetFields.size() > 1) {
            checkNullExpression = String.format("(%s != null && %s !=\"\") || (%s != null && %s !=\"\")",
                    targetFields.get(0), targetFields.get(0), targetFields.get(1), targetFields.get(1));
        } else {
            checkNullExpression = targetFields.get(0) + " != null  && " + "!" + targetFields.get(0) + ".equals(\"\")";
        }
        Node filterNullNode = input //
                .filter(checkNullExpression, new FieldList(targetFields));
        DuplicatedValueCheckAggregator aggregator = new DuplicatedValueCheckAggregator(targetFields,
                dupValWithStatusFlag);
        return filterNullNode.groupByAndAggregate(new FieldList(targetFields), aggregator, fms);
    }

    private static Node checkExceededCount(Node input, ExceededCountCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        boolean isMaxThreshold = param.getCntLessThanThresholdFlag();
        long threshold = param.getExceedCountThreshold();
        Node inputNodeCount = input.count(TOTAL_COUNT);
        inputNodeCount = inputNodeCount.apply(new ExceededCountCheckFunction(TOTAL_COUNT, threshold, isMaxThreshold),
                new FieldList(TOTAL_COUNT), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));

        return inputNodeCount;
    }

    private static Node checkEmptyField(Node input, EmptyFieldCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        List<String> keyField = param.getIdentifierFields();
        String checkEmptyField = param.getCheckEmptyField().toString();
        List<String> listOfFields = new ArrayList<String>();
        listOfFields.add((String) checkEmptyField);
        for (int i = 0; i < keyField.size(); i++) {
            if (!listOfFields.contains(keyField.get(i))) {
                listOfFields.add(keyField.get(i));
            }
        }
        Node nullFieldCheck = input.apply(new CheckFieldNotEmptyFunction(checkEmptyField, keyField),
                new FieldList(listOfFields), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return nullFieldCheck;
    }

    private static Node checkUnderPopulatedField(Node input, UnderPopulatedFieldCheckParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        String targetField = param.getGroupByFields().get(0);
        double threshold = param.getThreshold();
        Node inputTotalCount = input.count(TOTAL_COUNT) //
                .renamePipe("TotalCount");
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

    private static Node checkIncompleteCoverageForCol(Node input, IncompleteCoverageForColChkParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        String targetField = param.getGroupByFields().get(0);
        List<Object> expectedValues = param.getExpectedFieldValues();
        Node groupedInput = input //
                .groupByAndLimit(new FieldList(targetField), 1) //
                .addColumnWithFixedValue(DUMMY_GROUP, DUMMY_VALUE, String.class);
        IncompleteCoverageColCheckAggregator aggregator = new IncompleteCoverageColCheckAggregator(targetField,
                expectedValues);
        return groupedInput.groupByAndAggregate(new FieldList(DUMMY_GROUP), aggregator, fms);
    }

    private static Node checkOutOfCoverageValForRow(Node input, OutOfCoverageForRowChkParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input, param);
        String keyField = param.getKeyField();
        List<String> listOfFields = new ArrayList<String>();
        String targetField = param.getGroupByFields().get(0);
        listOfFields.add(targetField.toString());
        if (keyField.contains(",")) {
            String[] keyFieldList = keyField.split(",");
            for(int i = 0; i < keyFieldList.length; i++) {
                if (!listOfFields.contains(keyFieldList[i])) {
                    listOfFields.add(keyFieldList[i]);
                }
            }
        }
        List<Object> expectedFieldValues = param.getExpectedFieldValues();
        Node outOfCoverageRows = input.apply(
                new IncompleteCoverageRowCheckFunction(targetField, expectedFieldValues, keyField),
                new FieldList(listOfFields), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return outOfCoverageRows;
    }

    private static Node checkDiffVersionDomOnlyCount(List<Node> input, ExceedDomDiffBetwenVersionChkParam param) {
        List<FieldMetadata> fms = fieldMetadataPrep(input.get(0), param);
        String prevNotNullField = param.getPrevVersionNotEmptyField().toString();
        String currNotNullField = param.getCurrVersionNotEmptyField().toString();
        String prevNullField = param.getPrevVersionEmptyField().toString();
        String currNullField = param.getCurrVersionEmptyField().toString();
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
