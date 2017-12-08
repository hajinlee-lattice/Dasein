package com.latticeengines.datacloud.dataflow.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.UnexpectedValueCheckFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.CheckFieldNotNullFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.CheckFieldPopulationThresholdFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.CompreCntVersionsBusinesChkFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.CompreCntVersionsrDomOnlyChkFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.DuplicatedValueCheckAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.IncompleteCoverageGroupCheckAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.check.IncompleteCoverageRowCheckFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.CheckCode;
import com.latticeengines.domain.exposed.datacloud.check.CheckParam;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValuesWithStatusCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.EmptyOrNullFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.UnexpectedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceedDomDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceedCntDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.check.IncompleteCoverageGroupCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.IncompleteCoverageRowCheckParam;
import com.latticeengines.domain.exposed.datacloud.check.UnderPopulatedFieldCheckParam;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public final class CheckUtils {

    private final static String STATUS_ACTIVE = "ACTIVE";
    private final static String TOTAL_COUNT = "COUNT";
    private final static String PREV_TOTAL_COUNT = "PREV_" + TOTAL_COUNT;
    private final static String POPULATED_COUNT = "POPULATED";
    private final static String DUMMY_GROUP = "DUMMY_GROUP";
    private final static String DUMMY_VALUE = "DUMMY_VALUE";
    protected Map<String, Integer> namePositionMap;

    public static Node runCheck(List<Node> input, CheckParam param) {
        CheckCode checkCode = param.getCheckCode();
        switch (checkCode) {
            case DuplicatedValue:
                return checkDuplicatedValue(input.get(0), (DuplicatedValueCheckParam) param);
            case ExceededCount:
                return checkExceededCount(input.get(0), (UnexpectedValueCheckParam) param);
            case EmptyOrNullField:
                return checkEmptyOrNullField(input.get(0), (EmptyOrNullFieldCheckParam) param);
            case UnderPopulatedField:
                return checkUnderPopulatedField(input.get(0), (UnderPopulatedFieldCheckParam) param);
            case OutOfCoverageGroup:
                return checkOutOfCoverageGroup(input.get(0), (IncompleteCoverageGroupCheckParam) param);
            case OutOfCoverageRow:
                return checkOutOfCoverageRow(input.get(0), (IncompleteCoverageRowCheckParam) param);
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

    private static Node checkDuplicatedValueWithStatus(Node input, DuplicatedValuesWithStatusCheckParam param) {
        List<FieldMetadata> fms = Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        String statusField = param.getStatus();
        String targetField = param.getGroupByFields().get(0);
        // filtering records with status = "ACTIVE"
        String checkStatusExpression = String.format("%s.equals(\"%s\")", statusField, STATUS_ACTIVE);
        Node resultNode = input //
                .filter(checkStatusExpression, new FieldList(input.getFieldNames())) //
                .retain(new FieldList(param.getKeyField(), targetField));
        // checking duplicates check for the filtered records
        DuplicatedValueCheckAggregator aggregator = new DuplicatedValueCheckAggregator(targetField);
        return resultNode.groupByAndAggregate(new FieldList(targetField), aggregator, fms);
    }

    private static Node checkExceededCount(Node input, UnexpectedValueCheckParam param) {
        List<FieldMetadata> fms = Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        int maxThreshold = param.getExceedCountThreshold();
        Node inputNodeCount = input.count(TOTAL_COUNT);
        inputNodeCount = inputNodeCount.apply(new UnexpectedValueCheckFunction(TOTAL_COUNT, maxThreshold),
                new FieldList(TOTAL_COUNT), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));

        return inputNodeCount;
    }

    private static Node checkEmptyOrNullField(Node input, EmptyOrNullFieldCheckParam param) {
        List<FieldMetadata> fms = Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        String keyField = param.getKeyField();
        String checkNullField = param.getCheckNullField();
        Node nullFieldCheck = input.apply(new CheckFieldNotNullFunction(checkNullField, keyField),
                new FieldList(checkNullField, keyField), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return nullFieldCheck;
    }

    private static Node checkUnderPopulatedField(Node input, UnderPopulatedFieldCheckParam param) {
        List<FieldMetadata> fms = Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        String targetField = param.getGroupByFields().get(0);
        double threshold = param.getThreshold();
        Node inputTotalCount = input.count(TOTAL_COUNT) //
                .renamePipe("TotalCount");
        // filtering required field to check having empty/null value
        String checkStatusExpression = String.format("%s != null && %s != \"\"", targetField, targetField);
        Node resultNode = input //
                .filter(checkStatusExpression, new FieldList(input.getFieldNames()));
        Node populatedTotalCount = resultNode.count(POPULATED_COUNT) //
                .renamePipe("PopulatedCount");
        Node mergedInputNode = inputTotalCount //
                .combine(populatedTotalCount);
        mergedInputNode = mergedInputNode.apply(
                new CheckFieldPopulationThresholdFunction(TOTAL_COUNT, POPULATED_COUNT, threshold, targetField),
                new FieldList(TOTAL_COUNT, POPULATED_COUNT), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return mergedInputNode;
    }

    private static Node checkOutOfCoverageGroup(Node input, IncompleteCoverageGroupCheckParam param) {
        List<FieldMetadata> fms = Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        String targetField = param.getGroupByFields().get(0);
        List<String> coverageFieldList = param.getCoverageFields();
        Node groupedInput = input //
                .groupByAndLimit(new FieldList(targetField), 1) //
                .addColumnWithFixedValue(DUMMY_GROUP, DUMMY_VALUE, String.class);
        IncompleteCoverageGroupCheckAggregator aggregator = new IncompleteCoverageGroupCheckAggregator(targetField,
                coverageFieldList);
        return groupedInput.groupByAndAggregate(new FieldList(DUMMY_GROUP), aggregator, fms);
    }

    private static Node checkOutOfCoverageRow(Node input, IncompleteCoverageRowCheckParam param) {
        List<FieldMetadata> fms = Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        String keyField = param.getKeyField();
        String targetField = param.getGroupByFields().get(0);
        List<String> coverageFields = param.getCoverageFields();
        Node outOfCoverageRows = input.apply(
                new IncompleteCoverageRowCheckFunction(targetField, coverageFields, keyField),
                new FieldList(targetField, keyField), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_GROUP_ID, DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return outOfCoverageRows;
    }

    private static Node checkDiffVersionDomOnlyCount(List<Node> input, ExceedDomDiffBetwenVersionChkParam param) {
        List<FieldMetadata> fms = Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        String prevNotNullField = param.getPrevVersionNotNullField();
        String currNotNullField = param.getCurrVersionNotNullField();
        String prevNullField = param.getPrevVersionNullField();
        String currNullField = param.getCurrVersionNullField();
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
        List<FieldMetadata> fms = Arrays.asList(new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_CODE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_ROW_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_GROUP_ID, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_FIELD, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_VALUE, String.class), //
                new FieldMetadata(DataCloudConstants.CHK_ATTR_CHK_MSG, String.class) //
        );
        System.out.println("node1 : " + input.get(0).getPipeName() + " node2 : " + input.get(1).getPipeName());
        double threshold = param.getThreshold();
        Node prevVersionCount = input.get(0).count(PREV_TOTAL_COUNT) //
                .retain(new FieldList(PREV_TOTAL_COUNT));
        Node currVersionCount = input.get(1).count(TOTAL_COUNT) //
                .retain(new FieldList(TOTAL_COUNT));
        Node combinedCounts = prevVersionCount //
                .combine(currVersionCount);
        System.out.println("combined counts :" + combinedCounts.getFieldNamesArray());
        Node exceededBusinesses = combinedCounts.apply(
                new CompreCntVersionsBusinesChkFunction(PREV_TOTAL_COUNT, TOTAL_COUNT, threshold),
                new FieldList(PREV_TOTAL_COUNT, TOTAL_COUNT), fms,
                new FieldList(DataCloudConstants.CHK_ATTR_CHK_CODE, DataCloudConstants.CHK_ATTR_ROW_ID,
                        DataCloudConstants.CHK_ATTR_CHK_FIELD,
                        DataCloudConstants.CHK_ATTR_CHK_VALUE, DataCloudConstants.CHK_ATTR_CHK_MSG));
        return exceededBusinesses;
    }

}
