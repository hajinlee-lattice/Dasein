package com.latticeengines.datacloud.etl.transformation.transformer.impl.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.dataflow.transformation.source.ColumnCurationOperationFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractDataflowTransformer;
import com.latticeengines.domain.exposed.datacloud.dataflow.source.ColumnCurationParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.ColumnCurationConfig;

@Component(ColumnCurationTransformer.TRANSFORMER_NAME)
public class ColumnCurationTransformer
        extends AbstractDataflowTransformer<ColumnCurationConfig, ColumnCurationParameters> {

    private static final Logger log = LoggerFactory.getLogger(ColumnCurationTransformer.class);

    public static final String TRANSFORMER_NAME = "columnCurationTransformer";

    @Override
    protected String getDataFlowBeanName() {
        return ColumnCurationOperationFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Inject
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Override
    protected boolean validateConfig(ColumnCurationConfig config, List<String> baseSources) {
        String error = null;
        if (baseSources == null || (baseSources.size() != 1)) {
            error = "ColumnCurationTransformer can only process one source at a time";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        if ((config == null) || (config.getColumnOperations() == null)) {
            error = "The configuration is null or empty";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }

        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return ColumnCurationConfig.class;
    }

    @Override
    protected Class<ColumnCurationParameters> getDataFlowParametersClass() {
        return ColumnCurationParameters.class;
    }

    @Override
    protected void updateParameters(ColumnCurationParameters parameters, Source[] baseTemplates, Source targetTemplate,
            ColumnCurationConfig config, List<String> baseVersions) {
        // Map to store <operation type, related fields>
        Map<Calculation, List<String>> typeFieldMap = new HashMap<>();
        // Map to store <fields, value>
        Map<String, String> fieldValueMap = new HashMap<>();

        String sourceName = baseTemplates[0].getSourceName();
        log.info("SourceName: " + sourceName);

        Set<Calculation> opSet = new HashSet<>(Arrays.asList(config.getColumnOperations()));
        // Query into SourceColumn to update those HashMaps
        for (SourceColumn column : sourceColumnEntityMgr.getSourceColumns(sourceName)) {
            Calculation op = column.getCalculation();
            if (opSet.contains(op)) {
                List<String> fields = typeFieldMap.get(op);
                if (fields == null) {
                    fields = new LinkedList<>();
                    typeFieldMap.put(op, fields);
                }
                fields.add(column.getColumnName());

                fieldValueMap.put(column.getColumnName(), column.getArguments());
            }
        }

        parameters.setColumnOperations(config.getColumnOperations());
        parameters.setTypeFieldMap(typeFieldMap);
        parameters.setFieldValueMap(fieldValueMap);
    }
}
