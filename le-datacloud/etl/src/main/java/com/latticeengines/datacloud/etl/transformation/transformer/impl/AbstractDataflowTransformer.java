package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters.ENGINE_CONFIG;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.impl.SimpleTransformationDataFlowService;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class AbstractDataflowTransformer<T extends TransformerConfig, P extends TransformationFlowParameters>
        extends AbstractTransformer<T> {

    private static final Logger log = LoggerFactory.getLogger(AbstractDataflowTransformer.class);
    private static final ObjectMapper OM = new ObjectMapper();

    @Autowired
    protected SimpleTransformationDataFlowService dataFlowService;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected MetadataProxy metadataProxy;

    protected abstract String getDataFlowBeanName();

    @SuppressWarnings("unchecked")
    protected Class<P> getDataFlowParametersClass() {
        return (Class<P>) TransformationFlowParameters.class;
    }

    protected void updateParameters(P parameters, Source[] baseTemplates, Source targetTemplate, T configuration,
            List<String> baseVersions) {
        return;
    }

    protected P getParameters(TransformationProgress progress, Source[] baseSources, Source[] baseTemplates,
            Source targetTemplate, T configuration, String confJson, List<String> baseVersions) {
        P parameters;
        try {
            parameters = getDataFlowParametersClass().newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException("Failed construct a new progress object by empty constructor", e);
        }

        parameters.setConfJson(confJson);

        if (StringUtils.isNotBlank(confJson)) {
            JsonNode jsonNode = JsonUtils.deserialize(confJson, JsonNode.class);
            try {
                if (jsonNode.has(ENGINE_CONFIG)) {
                    TransformationFlowParameters.EngineConfiguration engineConfig = OM.treeToValue(
                            jsonNode.get(ENGINE_CONFIG), TransformationFlowParameters.EngineConfiguration.class);
                    parameters.setEngineConfiguration(engineConfig);
                    log.info("Loaded engine configuration: " + JsonUtils.serialize(engineConfig));
                }
            } catch (Exception e) {
                log.error("Failed to parse " + ENGINE_CONFIG + " from conf json.", e);
            }
        }

        parameters.setTimestampField(targetTemplate.getTimestampField());
        try {
            log.info("Progress version " + progress.getVersion());
            parameters.setTimestamp(HdfsPathBuilder.dateFormat.parse(progress.getVersion()));
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25012, e,
                    new String[] { targetTemplate.getSourceName(), e.getMessage() });
        }
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(targetTemplate.getSourceName()));

        List<String> baseTables = new ArrayList<String>();
        for (Source baseSource : baseSources) {
            baseTables.add(baseSource.getSourceName());
        }
        parameters.setBaseTables(baseTables);
        String[] primaryKey = targetTemplate.getPrimaryKey();
        if (primaryKey == null) {
            parameters.setPrimaryKeys(new ArrayList<>());
        } else {
            parameters.setPrimaryKeys(Arrays.asList(targetTemplate.getPrimaryKey()));
        }

        Map<String, String> templateSourceMap = new HashMap<>();

        for (int i = 0; i < baseTemplates.length; i++) {
            templateSourceMap.put(baseTemplates[i].getSourceName(), baseSources[i].getSourceName());
        }

        parameters.setTemplateSourceMap(templateSourceMap);

        updateParameters(parameters, baseTemplates, targetTemplate, configuration, baseVersions);
        return parameters;
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        try {
            Source[] baseSources = step.getBaseSources();
            List<String> baseSourceVersions = step.getBaseVersions();
            Source[] baseTemplates = step.getBaseTemplates();
            Source targetTemplate = step.getTargetTemplate();
            String confStr = step.getConfig();
            T configuration = getConfiguration(confStr);

            // The order of base sources in the source object should match with
            // the order of base versions in the configuration
            P parameters = getParameters(progress, baseSources, baseTemplates, targetTemplate, configuration, confStr,
                    baseSourceVersions);
            preDataFlowProcessing(step, workflowDir, parameters, configuration);
            Map<Source, List<String>> baseSourceVersionMap = setupBaseSourceVersionMap(step, parameters, configuration);
            Map<String, Table> baseTables = setupSourceTables(baseSourceVersionMap);
            step.setBaseTables(baseTables);
            Table result = dataFlowService.executeDataFlow(step, getDataFlowBeanName(), parameters, workflowDir);
            step.setCount(result.getCount());
            List<Schema> baseSchemas = getBaseSourceSchemas(step);
            step.setTargetSchema(getTargetSchema(result, parameters, baseSchemas));
            postDataFlowProcessing(workflowDir, parameters, configuration);
            updateStepCount(step, workflowDir);
        } catch (Exception e) {
            log.error("Failed to transform data", e);
            return false;
        }

        return true;
    }

    private List<Schema> getBaseSourceSchemas(TransformStep step) {
        Transformer transformer = step.getTransformer();
        if (!(transformer instanceof AbstractDataflowTransformer)) {
            return null;
        }
        boolean needAvsc = ((AbstractDataflowTransformer) transformer).needBaseAvsc();
        if (needAvsc) {
            Source[] baseSources = step.getBaseSources();
            List<String> baseSourceVersions = step.getBaseVersions();
            List<Schema> schemas = new ArrayList<>();
            for (int i = 0; i < baseSources.length; i++) {
                Source source = baseSources[i];
                String version = baseSourceVersions.get(i);
                Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(source.getSourceName(), version);
                schemas.add(schema);
            }
            return schemas;
        } else {
            return null;
        }
    }

    protected Map<Source, List<String>> setupBaseSourceVersionMap(TransformStep step, P parameters, T configuration) {
        Map<Source, List<String>> baseSourceVersionMap = new HashMap<Source, List<String>>();
        for (int i = 0; i < step.getBaseSources().length; i++) {
            Source baseSource = step.getBaseSources()[i];
            List<String> versionList = baseSourceVersionMap.get(baseSource);
            if (versionList == null) {
                versionList = new ArrayList<>();
                baseSourceVersionMap.put(baseSource, versionList);
            }
            versionList.add(step.getBaseVersions().get(i));
        }
        return baseSourceVersionMap;
    }

    protected Map<String, Table> setupSourceTables(Map<Source, List<String>> baseSourceVersions) {
        Map<String, Table> sourceTables = new HashMap<>();
        for (Map.Entry<Source, List<String>> entry : baseSourceVersions.entrySet()) {
            Source baseSource = entry.getKey();
            List<String> baseSourceVersion = entry.getValue();
            log.info("Add base source " + baseSource.getSourceName());
            addSource(sourceTables, baseSource, baseSourceVersion);
        }
        return sourceTables;
    }

    protected boolean addSource(Map<String, Table> sourceTables, Source source, List<String> versions) {
        String sourceName = source.getSourceName();
        Table sourceTable;
        try {
            if (source instanceof TableSource) {
                TableSource tableSource = (TableSource) source;
                sourceTable = metadataProxy.getTable(tableSource.getCustomerSpace().toString(),
                        tableSource.getTable().getName());
            } else if (versions.size() == 1) {
                sourceTable = hdfsSourceEntityMgr.getTableAtVersion(source, versions.get(0));
            } else {
                sourceTable = hdfsSourceEntityMgr.getTableAtVersions(source, versions);
            }
            log.info("Select source " + sourceName + "@versions " + StringUtils.join(versions, ","));
        } catch (Exception e) {
            log.error("Source " + sourceName + " is not initiated in HDFS", e);
            return false;
        }
        sourceTables.put(sourceName, sourceTable);
        return true;
    }

    @Override
    protected boolean validateConfig(T config, List<String> sourceNames) {
        return true;
    }

    protected boolean needBaseAvsc() {
        return false;
    }

    protected Schema getTargetSchema(Table result, P parameters, List<Schema> baseSchemas) {
        return null;
    }

    protected void preDataFlowProcessing(TransformStep step, String workflowDir, P parameters, T configuration) {}

    protected void postDataFlowProcessing(String workflowDir, P parameters, T configuration) {}

    protected void updateStepCount(TransformStep step, String workflowDir) {
    }

}
