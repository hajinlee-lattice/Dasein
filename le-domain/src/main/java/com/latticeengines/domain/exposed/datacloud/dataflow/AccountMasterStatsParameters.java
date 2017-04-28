package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.metadata.FundamentalType;

public class AccountMasterStatsParameters extends TransformationFlowParameters {
    public static final String DIMENSION_COLUMN_PREPOSTFIX = "_";
    public static final String LBL_ORDER_POST = ">";
    public static final String LBL_ORDER_PRE_ENCODED_YES = "[EY";
    public static final String LBL_ORDER_PRE_ENCODED_NO = "[EN";
    public static final String LBL_ORDER_PRE_NUMERIC = "[N";
    public static final String LBL_ORDER_PRE_BOOLEAN = "[B";
    public static final String LBL_ORDER_PRE_OBJECT = "[O";
    public static final String COUNT_KEY = "_COUNT_";
    public static final String MIN_MAX_KEY = "_MinMax_";
    public static final String GROUP_TOTAL_KEY_TEMP = "_GroupTotal_";
    public static final String GROUP_TOTAL_KEY = "GroupTotal";
    public static final String ATTR_COUNT_1_KEY = "AttrCount1";
    public static final String ATTR_COUNT_2_KEY = "AttrCount2";
    public static final String ATTR_COUNT_3_KEY = "AttrCount3";
    public static final String ATTR_COUNT_4_KEY = "AttrCount4";
    public static final String PID_KEY = "PID";
    public static final String DATA_CLOUD_VERSION = "DataCloudVersion";
    public static final String ENCODED_NO = "NO";
    public static final String ENCODED_YES = "YES";
    public static final String DUNS = "LDC_DUNS";
    public static final String DDUNS = "DOMESTIC_ULTIMATE_DUNS_NUMBER";
    public static final String GDUNS = "GLOBAL_ULTIMATE_DUNS_NUMBER";
    public static final String HQ_DUNS = "HQ_DUNS";
    public static final String STATUS_CODE = "STATUS_CODE";
    public static final String SUBSIDIARY_INDICATOR = "SUBSIDIARY_INDICATOR";
    public static final Integer DEFAULT_MAX_BUCKET_COUNT = 5;

    private Map<String, List<String>> dimensionDefinitionMap;

    private Map<String, String> attributeCategoryMap;

    private Map<String, Map<String, Long>> dimensionValuesIdMap;

    private String cubeColumnName;

    private List<String> finalDimensionColumns;

    private Map<String, CategoricalDimension> requiredDimensions;

    private Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap;

    private Map<String, Long> rootIdsForNonRequiredDimensions;

    private Map<FundamentalType, List<String>> typeFieldMap;

    private List<String> encodedColumns;

    private int maxBucketCount = DEFAULT_MAX_BUCKET_COUNT;

    private List<String> columnsForStatsCalculation;

    private List<Integer> columnIdsForStatsCalculation;

    private boolean numericalBucketsRequired;

    private String dataCloudVersion;

    public Map<String, List<String>> getDimensionDefinitionMap() {
        return dimensionDefinitionMap;
    }

    public void setDimensionDefinitionMap(Map<String, List<String>> dimensionDefinitionMap) {
        this.dimensionDefinitionMap = dimensionDefinitionMap;
    }

    public Map<String, String> getAttributeCategoryMap() {
        return attributeCategoryMap;
    }

    public void setAttributeCategoryMap(Map<String, String> attributeCategoryMap) {
        this.attributeCategoryMap = attributeCategoryMap;
    }

    public Map<String, Map<String, Long>> getDimensionValuesIdMap() {
        return dimensionValuesIdMap;
    }

    public void setDimensionValuesIdMap(Map<String, Map<String, Long>> dimensionValuesIdMap) {
        this.dimensionValuesIdMap = dimensionValuesIdMap;
    }

    public String getCubeColumnName() {
        return cubeColumnName;
    }

    public void setCubeColumnName(String cubeColumnName) {
        this.cubeColumnName = cubeColumnName;
    }

    public List<String> getFinalDimensionColumns() {
        return finalDimensionColumns;
    }

    public void setFinalDimensionColumns(List<String> finalDimensionColumns) {
        this.finalDimensionColumns = finalDimensionColumns;
    }

    public Map<String, CategoricalDimension> getRequiredDimensions() {
        return requiredDimensions;
    }

    public void setRequiredDimensions(Map<String, CategoricalDimension> requiredDimensions) {
        this.requiredDimensions = requiredDimensions;
    }

    public Map<FundamentalType, List<String>> getTypeFieldMap() {
        return typeFieldMap;
    }

    public void setTypeFieldMap(Map<FundamentalType, List<String>> typeFieldMap) {
        this.typeFieldMap = typeFieldMap;
    }

    public Map<String, Map<String, CategoricalAttribute>> getRequiredDimensionsValuesMap() {
        return requiredDimensionsValuesMap;
    }

    public void setRequiredDimensionsValuesMap(
            Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap) {
        this.requiredDimensionsValuesMap = requiredDimensionsValuesMap;
    }

    public Map<String, Long> getRootIdsForNonRequiredDimensions() {
        return rootIdsForNonRequiredDimensions;
    }

    public void setRootIdsForNonRequiredDimensions(Map<String, Long> rootIdsForNonRequiredDimensions) {
        this.rootIdsForNonRequiredDimensions = rootIdsForNonRequiredDimensions;
    }

    public int getMaxBucketCount() {
        return maxBucketCount;
    }

    public void setMaxBucketCount(int maxBucketCount) {
        this.maxBucketCount = maxBucketCount;
    }

    public List<String> getEncodedColumns() {
        return encodedColumns;
    }

    public void setEncodedColumns(List<String> encodedColumns) {
        this.encodedColumns = encodedColumns;
    }

    public List<String> getColumnsForStatsCalculation() {
        return columnsForStatsCalculation;
    }

    public void setColumnsForStatsCalculation(List<String> columnsForStatsCalculation) {
        this.columnsForStatsCalculation = columnsForStatsCalculation;
    }

    public List<Integer> getColumnIdsForStatsCalculation() {
        return columnIdsForStatsCalculation;
    }

    public void setColumnIdsForStatsCalculation(List<Integer> columnIdsForStatsCalculation) {
        this.columnIdsForStatsCalculation = columnIdsForStatsCalculation;
    }

    public boolean isNumericalBucketsRequired() {
        return numericalBucketsRequired;
    }

    public void setNumericalBucketsRequired(boolean numericalBucketsRequired) {
        this.numericalBucketsRequired = numericalBucketsRequired;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

}
