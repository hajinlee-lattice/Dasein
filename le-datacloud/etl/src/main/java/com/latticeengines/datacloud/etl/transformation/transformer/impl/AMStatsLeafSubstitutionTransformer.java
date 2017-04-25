package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterReport;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterStatisticsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.DimensionAttributeProxy;

@Component("amStatsLeafSubstitutionTransformer")
public class AMStatsLeafSubstitutionTransformer
        extends AbstractDataflowTransformer<AccountMasterStatisticsConfig, AccountMasterStatsParameters> {
    private static final Log log = LogFactory.getLog(AMStatsLeafSubstitutionTransformer.class);

    @Autowired
    private DimensionAttributeProxy dimensionAttributeProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    private AccountMasterReport accountMasterReport;

    @Override
    public String getName() {
        return "amStatsLeafSubstitutionTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsLeafSubstitutionFlow";
    }

    @Override
    public boolean validateConfig(AccountMasterStatisticsConfig config, List<String> baseSources) {
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return AccountMasterStatisticsConfig.class;
    }

    @Override
    protected Class<AccountMasterStatsParameters> getDataFlowParametersClass() {
        return AccountMasterStatsParameters.class;
    }

    @Override
    protected void updateParameters(AccountMasterStatsParameters parameters, Source[] baseTemplates,
            Source targetTemplate, AccountMasterStatisticsConfig config) {
        List<String> dimensions = config.getDimensions();

        List<CategoricalDimension> allDimensions = getAllDimensions();
        List<String> finalDimensionColumns = new ArrayList<>();

        Map<String, CategoricalDimension> requiredDimensions = new HashMap<>();
        Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap = new HashMap<>();
        Map<String, List<String>> dimensionDefinitionMap = new HashMap<>();

        Map<String, Long> rootIdsForNonRequiredDimensions = new HashMap<>();

        for (CategoricalDimension dimension : allDimensions) {
            finalDimensionColumns.add(dimension.getDimension());
            if (dimensions.contains(dimension.getDimension())) {
                requiredDimensions.put(dimension.getDimension(), dimension);
                List<CategoricalAttribute> dimensionAttrDetails = //
                        getAllAttributes(dimension.getRootAttrId());
                List<String> dimensionDetails = new ArrayList<>();
                dimensionDetails.add(dimensionAttrDetails.get(0).getAttrName());
                dimensionDefinitionMap.put(//
                        AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX //
                                + dimension.getDimension()//
                                + AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX, //
                        dimensionDetails);

                Map<String, CategoricalAttribute> dimensionValuesMap = new HashMap<>();
                for (CategoricalAttribute attr : dimensionAttrDetails) {
                    dimensionValuesMap.put(attr.getAttrValue(), attr);
                }
                requiredDimensionsValuesMap.put(dimension.getDimension(), dimensionValuesMap);
            } else {
                Long rootId = dimension.getRootAttrId() == null //
                        ? dimension.getPid() //
                        : dimension.getRootAttrId();
                rootIdsForNonRequiredDimensions.put(dimension.getDimension(), rootId);
            }
        }

        String dataCloudVersion = config.getDataCloudVersion();
        if (dataCloudVersion == null) {
            String currentDataCloudVersion = //
                    columnMetadataProxy.latestVersion(null).getVersion();
            dataCloudVersion = currentDataCloudVersion;
        }
        List<ColumnMetadata> columnMetadatas = //
                columnMetadataProxy.columnSelection(Predefined.Enrichment, dataCloudVersion);

        Map<FundamentalType, List<String>> typeFieldMap = new HashMap<>();
        Set<String> encodedColumns = new HashSet<>();
        ObjectMapper objectMapper = new ObjectMapper();

        typeFieldMap.put(FundamentalType.BOOLEAN, new ArrayList<>());
        typeFieldMap.put(FundamentalType.ENUM, new ArrayList<>());

        Set<FundamentalType> uniqueTypes = new HashSet<>();

        for (ColumnMetadata meta : columnMetadatas) {
            FundamentalType type = meta.getFundamentalType();
            String name = meta.getColumnName();
            uniqueTypes.add(type);
            List<String> fieldList = typeFieldMap.get(type);
            if (type == FundamentalType.BOOLEAN || type == FundamentalType.ENUM) {
                if (StringUtils.isEmpty(meta.getDecodeStrategy())) {
                    fieldList.add(name);
                } else {
                    parseEncodedColumnsMetadata(encodedColumns, objectMapper, meta);
                }
            }
        }

        parameters.setTypeFieldMap(typeFieldMap);
        parameters.setEncodedColumns(new ArrayList<String>(encodedColumns));

        String originalBaseSourceName = //
                accountMasterReport.getBaseSources()[0].getSourceName();

        HashSet<String> excludeCols = new HashSet<String>();
        String[] excludeAttrs = accountMasterReport.getExcludeAttrs();
        for (int i = 0; i < excludeAttrs.length; i++) {
            excludeCols.add(excludeAttrs[i]);
        }

        List<String> columnsForStatsCalculation = new ArrayList<String>();
        List<Integer> columnIdsForStatsCalculation = new ArrayList<Integer>();
        List<SourceColumn> sourceColumns = //
                sourceColumnEntityMgr.getSourceColumns(originalBaseSourceName);

        for (int i = 0; i < sourceColumns.size(); i++) {
            SourceColumn col = sourceColumns.get(i);
            String attr = col.getColumnName();
            if (excludeCols.contains(attr)) {
                continue;
            }
            Integer attrId = col.getCharAttrId();
            if (attrId == null) {
                log.info("Skip attr " + attr + " without attr id");
                continue;
            }

            columnsForStatsCalculation.add(attr);
            columnIdsForStatsCalculation.add(attrId);
        }

        parameters.setColumnsForStatsCalculation(columnsForStatsCalculation);
        parameters.setColumnIdsForStatsCalculation(columnIdsForStatsCalculation);
        parameters.setAttributeCategoryMap(config.getAttributeCategoryMap());
        parameters.setCubeColumnName(config.getCubeColumnName());
        parameters.setDimensionDefinitionMap(dimensionDefinitionMap);
        parameters.setDimensionValuesIdMap(config.getDimensionValuesIdMap());
        parameters.setFinalDimensionColumns(finalDimensionColumns);
        parameters.setRequiredDimensions(requiredDimensions);
        parameters.setRequiredDimensionsValuesMap(requiredDimensionsValuesMap);
        parameters.setRootIdsForNonRequiredDimensions(rootIdsForNonRequiredDimensions);
        parameters.setNumericalBucketsRequired(config.isNumericalBucketsRequired());
        parameters.setDataCloudVersion(dataCloudVersion);
    }

    private void parseEncodedColumnsMetadata(Set<String> encodedColumns, //
            ObjectMapper objectMapper, ColumnMetadata meta) {
        String decodeStrategyStr = meta.getDecodeStrategy();
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(decodeStrategyStr);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse decodeStrategy " + decodeStrategyStr);
        }
        String encodedColumn = jsonNode.has("EncodedColumn") //
                ? jsonNode.get("EncodedColumn").asText() : null;

        encodedColumns.add(encodedColumn);
    }

    private List<CategoricalDimension> getAllDimensions() {
        List<CategoricalDimension> allDimensions = //
                dimensionAttributeProxy.getAllDimensions();
        return allDimensions;
    }

    private List<CategoricalAttribute> getAllAttributes(Long rootId) {
        List<CategoricalAttribute> allAttributes = //
                dimensionAttributeProxy.getAllAttributes(rootId);
        return allAttributes;
    }

}
