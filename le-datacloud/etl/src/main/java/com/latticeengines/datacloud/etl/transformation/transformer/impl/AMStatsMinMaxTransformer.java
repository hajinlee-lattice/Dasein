package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterStatisticsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.proxy.exposed.matchapi.DimensionAttributeProxy;

@Component("amStatsMinMaxTransformer")
public class AMStatsMinMaxTransformer
        extends AbstractDataflowTransformer<AccountMasterStatisticsConfig, AccountMasterStatsParameters> {
    @Autowired
    private DimensionAttributeProxy dimensionAttributeProxy;

    @Override
    public String getName() {
        return "amStatsMinMaxTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsMinMaxFlow";
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
