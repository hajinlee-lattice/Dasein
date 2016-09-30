package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BasicTransformationConfiguration;


/**
 * This is the base implementation of the transformatin service
 * for simpliest sources: single base source, single dataflow.
 */
public abstract class SimpleTransformationServiceBase<T extends TransformationConfiguration, P extends TransformationFlowParameters>
        extends AbstractTransformationService<T> {

    @Autowired
    protected SimpleTransformationDataFlowService dataFlowService;

    protected abstract String getDataFlowBeanName();

    protected abstract String getServiceBeanName();

    //!! override this if the parameter class is not TransformationFlowParameters
    @SuppressWarnings("unchecked")
    protected Class<P> getDataFlowParametersClass() {
        return (Class<P>) TransformationFlowParameters.class;
    }

    //!! override this if the parameter class is not BasicTransformationConfiguration
    @SuppressWarnings("unchecked")
    public Class<T> getConfigurationClass() {
        return (Class<T>) BasicTransformationConfiguration.class;
    }

    @Override
    public boolean isManualTriggerred() {
        return true;
    }

    @Override
    public List<String> findUnprocessedBaseVersions() {
        DerivedSource source = (DerivedSource) getSource();
        Source baseSource = source.getBaseSources()[0];
        String latestBaseVersion = hdfsSourceEntityMgr.getCurrentVersion(baseSource);
        if (progressEntityMgr.hasActiveForBaseSourceVersions(source, latestBaseVersion)) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(latestBaseVersion);
        }
    }

    protected P getDataFlowParameters(TransformationProgress progress, T transConf) {
        P parameters;
        try {
            parameters = getDataFlowParametersClass().newInstance();
        } catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException("Failed construct a new progress object by empty constructor", e);
        }

        parameters.setTimestampField(getSource().getTimestampField());
        try {
            parameters.setTimestamp(HdfsPathBuilder.dateFormat.parse(progress.getVersion()));
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25012, e,
                    new String[] { getSource().getSourceName(), e.getMessage() });
        }
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(getSource().getSourceName()));

        DerivedSource derivedSource = (DerivedSource) getSource();
        parameters.setBaseTables(Collections.singletonList(derivedSource.getBaseSources()[0].getSourceName()));
        parameters.setPrimaryKeys(Arrays.asList(getSource().getPrimaryKey()));
        return parameters;
    }

    @Override
    public T createTransformationConfiguration(List<String> baseVersionsToProcess, String targetVersion) {
        if (StringUtils.isEmpty(targetVersion)) {
            targetVersion = createNewVersionStringFromNow();
        }

        T configuration;
        try {
            configuration = getConfigurationClass().newInstance();
        } catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException("Failed construct a new configuration object by empty constructor", e);
        }

        configuration.setSourceName(getSource().getSourceName());
        configuration.setServiceBeanName(getServiceBeanName());
        configuration.setVersion(targetVersion);
        configuration.setBaseVersions(baseVersionsToProcess.get(0));
        configuration.setSourceColumns(sourceColumnEntityMgr.getSourceColumns(getSource().getSourceName()));
        return configuration;
    }

    protected TransformationProgress transformHook(TransformationProgress progress, T transConf) {
        String workflowDir = initialDataFlowDirInHdfs(progress);
        if (!cleanupHdfsDir(workflowDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + workflowDir, null);
            return null;
        }
        try {
            Map<Source, String> baseSourceVersionMap = new HashMap<>();
            Source baseSource = ((DerivedSource) getSource()).getBaseSources()[0];

            // first try to get base version from progress
            String baseSourceVersion = progress.getBaseSourceVersions();

            // then try transformation configuration
            if (StringUtils.isEmpty(baseSourceVersion)) {
                baseSourceVersion = transConf.getBaseVersions();
            }
            // finally get current version as default
            if (StringUtils.isEmpty(baseSourceVersion)) {
                baseSourceVersion = hdfsSourceEntityMgr.getCurrentVersion(baseSource);
            }

            baseSourceVersionMap.put(baseSource, baseSourceVersion);

            P parameters = getDataFlowParameters(progress, transConf);

            dataFlowService.executeDataFlow(getSource(), workflowDir, baseSourceVersionMap, getDataFlowBeanName(),
                    parameters);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform data.", e);
            return null;
        }

        if (doPostProcessing(progress, workflowDir)) {
            return progress;
        } else {
            return null;
        }
    }

    protected P enrichStandardDataFlowParameters(P parameters, T configuration, TransformationProgress progress) {
        parameters.setTimestampField(getSource().getTimestampField());
        try {
            parameters.setTimestamp(HdfsPathBuilder.dateFormat.parse(progress.getVersion()));
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25012, e,
                    new String[] { getSource().getSourceName(), e.getMessage() });
        }
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(getSource().getSourceName()));

        DerivedSource derivedSource = (DerivedSource) getSource();
        parameters.setBaseTables(Collections.singletonList(derivedSource.getBaseSources()[0].getSourceName()));
        parameters.setPrimaryKeys(Arrays.asList(getSource().getPrimaryKey()));
        return parameters;
    }

    protected Date checkTransformationConfigurationValidity(T conf) {
        if (conf.getSourceConfigurations() == null) {
            conf.setSourceConfigurations(new HashMap<String, String>());
        }
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());
        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @SuppressWarnings("unchecked")
    protected T parseTransConfJsonInsideWorkflow(String confStr) throws IOException {
        return JsonUtils.deserialize(confStr, (Class<T>) getConfigurationClass());
    }

}
