package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.impl.SimpleTransformationDataFlowService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public abstract class AbstractDataflowTransformer<T extends TransformerConfig, P extends TransformationFlowParameters>
                                   extends AbstractTransformer<T> {

    private static final Log log = LogFactory.getLog(AbstractDataflowTransformer.class);

    @Autowired
    protected SimpleTransformationDataFlowService dataFlowService;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    protected abstract String getDataFlowBeanName();

    @SuppressWarnings("unchecked")
    protected Class<P> getDataFlowParametersClass() {
        return (Class<P>) TransformationFlowParameters.class;
    }

    protected void updateParameters(P parameters, Source[] baseTemplates, Source targetTemplate, T configuration) {
        return;
    }

    private P getParameters(TransformationProgress progress, Source[] baseSources, Source[] baseTemplates, Source targetTemplate, T configuration,
                            String confJson) {
        P parameters;
        try {
            parameters = getDataFlowParametersClass().newInstance();
        } catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException("Failed construct a new progress object by empty constructor", e);
        }

        parameters.setConfJson(confJson);

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
            parameters.setPrimaryKeys(new ArrayList<String>());
        } else{
            parameters.setPrimaryKeys(Arrays.asList(targetTemplate.getPrimaryKey()));
        }

        Map<String, String> templateSourceMap = new HashMap<String, String>();

        for (int i = 0; i < baseTemplates.length; i++) {
            templateSourceMap.put(baseTemplates[i].getSourceName(), baseSources[i].getSourceName());
        }

        parameters.setTemplateSourceMap(templateSourceMap);

        updateParameters(parameters, baseTemplates, targetTemplate, configuration);
        return parameters;
    }

    @Override
    protected boolean transform(TransformationProgress progress, String workflowDir, Source[] baseSources, List<String> baseSourceVersions,
                                Source[] baseTemplates, Source targetTemplate, T configuration, String confStr) {
        try {
            // The order of base sources in the source object should match with
            // the order of base versions in the configuration
            P parameters = getParameters(progress, baseSources, baseTemplates, targetTemplate, configuration, confStr);
            Map<Source, List<String>> baseSourceVersionMap = new HashMap<Source, List<String>>();
            for (int i = 0; i < baseSources.length; i++) {
                Source baseSource = baseSources[i];

                List<String> versionList = baseSourceVersionMap.get(baseSource);
                if (versionList == null) {
                    versionList = new ArrayList<String>();
                    baseSourceVersionMap.put(baseSource, versionList);
                }
                versionList.add(baseSourceVersions.get(i));
            }

            dataFlowService.executeDataFlow(targetTemplate, workflowDir, baseSourceVersionMap, getDataFlowBeanName(),
                    parameters);
        } catch (Exception e) {
            log.error ("Failed to transform data", e);
            return false;
        }

        return true;
    }
}
