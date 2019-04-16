package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;


/**
 * This is the base implementation of the transformatin service
 * For simpliest sources: single dataflow.
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
    @Override
    @SuppressWarnings("unchecked")
    public Class<T> getConfigurationClass() {
        return (Class<T>) BasicTransformationConfiguration.class;
    }

    //!! override this if there are multiple base versions for one base source
    protected List<String> parseBaseVersions(Source baseSource, String baseVersion) {
        List<String> baseVersionList = new ArrayList<String>();
        baseVersionList.add(baseVersion);
        return baseVersionList;
    }

    @Override
    public boolean isManualTriggerred() {
        return true;
    }

    //! override this if there are multiple base versions for one base source
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
        if (derivedSource.getBaseSources().length == 1) {
            parameters.setBaseTables(Collections.singletonList(derivedSource.getBaseSources()[0].getSourceName()));
        } else {
            List<String> baseTables = new ArrayList<String>();
            for (Source baseSource : derivedSource.getBaseSources()) {
                baseTables.add(baseSource.getSourceName());
            }
            parameters.setBaseTables(baseTables);
        }
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
        configuration.setBaseVersions(baseVersionsToProcess);
        configuration.setSourceColumns(sourceColumnEntityMgr.getSourceColumns(getSource().getSourceName()));
        return configuration;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected TransformationProgress transformHook(TransformationProgress progress, T transConf) {
        String workflowDir = initialDataFlowDirInHdfs(progress);
        if (!cleanupHdfsDir(workflowDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + workflowDir, null);
            return null;
        }
        try {
            // The order of base sources in the source object should match with
            // the order of base versions in the configuration
            if (transConf.getBaseVersions() != null
                    && transConf.getBaseVersions().size() != ((DerivedSource) getSource()).getBaseSources().length) {
                updateStatusToFailed(progress, "Number of base versions is different with number of base sources.",
                        null);
                return null;
            }
            Map<Source, List<String>> baseSourceVersionMap = new HashMap<Source, List<String>>();
            for (int i = 0; i < ((DerivedSource) getSource()).getBaseSources().length; i++) {
                Source baseSource = ((DerivedSource) getSource()).getBaseSources()[i];

                String baseSourceVersion = null;
                // first try transformation configuration
                if (!CollectionUtils.isEmpty(transConf.getBaseVersions()))
                    baseSourceVersion = transConf.getBaseVersions().get(i);

                // then get current version as default
                if (StringUtils.isEmpty(baseSourceVersion)) {
                    baseSourceVersion = hdfsSourceEntityMgr.getCurrentVersion(baseSource);
                }
                List<String> baseSourceVersionList = parseBaseVersions(baseSource, baseSourceVersion);
                baseSourceVersionMap.put(baseSource, baseSourceVersionList);
            }

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

    @Override
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

    @Override
    protected T parseTransConfJsonInsideWorkflow(String confStr) throws IOException {
        return JsonUtils.deserialize(confStr, getConfigurationClass());
    }

}
