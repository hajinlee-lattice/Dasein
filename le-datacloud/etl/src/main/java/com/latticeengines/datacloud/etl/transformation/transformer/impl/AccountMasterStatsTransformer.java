package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterStatisticsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.DimensionAttributeProxy;

@Component("accountMasterStatsTransformer")
public class AccountMasterStatsTransformer
        extends AbstractDataflowTransformer<AccountMasterStatisticsConfig, AccountMasterStatsParameters> {
    @Autowired
    private DimensionAttributeProxy dimensionAttributeProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Override
    public String getName() {
        return "accountMasterStatsTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "accountMasterStatsFlow";
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
                List<CategoricalAttribute> dimensionAttrDetails = getAllAttributes(dimension.getRootAttrId());
                List<String> dimensionDetails = new ArrayList<>();
                dimensionDetails.add(dimensionAttrDetails.get(0).getAttrName());
                dimensionDefinitionMap.put(AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX
                        + dimension.getDimension() + AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX,
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

        String currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        List<ColumnMetadata> columnMetadatas = columnMetadataProxy.columnSelection(Predefined.Enrichment,
                currentDataCloudVersion);
        Map<FundamentalType, List<String>> typeFieldMap = new HashMap<>();
        Set<String> encodedColumns = new HashSet<>();
        ObjectMapper objectMapper = new ObjectMapper();

        typeFieldMap.put(FundamentalType.BOOLEAN, new ArrayList<>());
        typeFieldMap.put(FundamentalType.ENUM, new ArrayList<>());
        for (ColumnMetadata meta : columnMetadatas) {
            FundamentalType type = meta.getFundamentalType();
            String name = meta.getColumnName();
            List<String> fieldList = typeFieldMap.get(type);
            if (type == FundamentalType.BOOLEAN || type == FundamentalType.ENUM) {
                if (meta.getDecodeStrategy() == null) {
                    fieldList.add(name);
                } else {
                    String decodeStrategyStr = meta.getDecodeStrategy();
                    if (StringUtils.isEmpty(decodeStrategyStr)) {
                        continue;
                    }
                    JsonNode jsonNode;
                    try {
                        jsonNode = objectMapper.readTree(decodeStrategyStr);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to parse decodeStrategy " + decodeStrategyStr);
                    }
                    String encodedColumn = jsonNode.has("EncodedColumn") ? jsonNode.get("EncodedColumn").asText()
                            : null;

                    encodedColumns.add(encodedColumn);
                }
            }
        }
        parameters.setTypeFieldMap(typeFieldMap);
        parameters.setEncodedColumns(new ArrayList<String>(encodedColumns));

        parameters.setAttributeCategoryMap(config.getAttributeCategoryMap());
        parameters.setCubeColumnName(config.getCubeColumnName());
        parameters.setDimensionDefinitionMap(dimensionDefinitionMap);
        parameters.setDimensionValuesIdMap(config.getDimensionValuesIdMap());
        parameters.setFinalDimensionColumns(finalDimensionColumns);
        parameters.setRequiredDimensions(requiredDimensions);
        parameters.setRequiredDimensionsValuesMap(requiredDimensionsValuesMap);
        parameters.setRootIdsForNonRequiredDimensions(rootIdsForNonRequiredDimensions);
    }

    private List<CategoricalDimension> getAllDimensions() {
        List<CategoricalDimension> allDimensions = dimensionAttributeProxy.getAllDimensions();
        return allDimensions;
    }

    private List<CategoricalAttribute> getAllAttributes(Long rootId) {
        List<CategoricalAttribute> allAttributes = dimensionAttributeProxy.getAllAttributes(rootId);
        return allAttributes;
    }

}
