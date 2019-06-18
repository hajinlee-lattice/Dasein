package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.ProgressHelper;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

public abstract class AbstractTransformer<T extends TransformerConfig> implements Transformer {

    private static final Logger log = LoggerFactory.getLogger(AbstractTransformer.class);

    @Inject
    protected TransformationProgressEntityMgr progressEntityMgr;

    @Inject
    private ProgressHelper progressHelper;

    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;

    protected T configuration;

    @Override
    public abstract String getName();

    protected abstract boolean validateConfig(T config, List<String> sourceNames);

    protected abstract boolean transformInternal(TransformationProgress progress, String workflowDir,
            TransformStep step);

    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return TransformerConfig.class;
    }

    @Override
    public boolean validateConfig(String confStr, List<String> sourceNames) {
        if (getConfigurationClass() == TransformerConfig.class) {
            return true;
        } else {
            T config = getConfiguration(confStr);
            if (config == null) {
                return false;
            } else {
                configuration = config;
                return validateConfig(configuration, sourceNames);
            }
        }
    }

    @Override
    public void initBaseSources(String confStr, List<String> sourceNames) {
        T config = getConfiguration(confStr);
        if (config == null) {
            return;
        } else {
            initBaseSources(config, sourceNames);
        }
    }

    protected void initBaseSources(T config, List<String> sourceNames) {

    }

    protected String getSourceHdfsDir(TransformStep step, int sourceIdx) {
        Source source = step.getBaseSources()[sourceIdx];

        List<String> baseSourceVersions = step.getBaseVersions();
        String sourceDirInHdfs = null;
        if (!(source instanceof TableSource)) {
            sourceDirInHdfs = hdfsPathBuilder
                    .constructTransformationSourceDir(source, baseSourceVersions.get(sourceIdx)).toString();
        } else {
            Table table = ((TableSource) source).getTable();
            if (table.getExtracts().size() > 1) {
                throw new IllegalArgumentException("Can only handle single extract table.");
            }
            sourceDirInHdfs = table.getExtracts().get(0).getPath();
            if (sourceDirInHdfs.endsWith(".avro")) {
                sourceDirInHdfs = sourceDirInHdfs.substring(0, sourceDirInHdfs.lastIndexOf("/"));
            } else {
                sourceDirInHdfs = sourceDirInHdfs.endsWith("/")
                        ? sourceDirInHdfs.substring(0, sourceDirInHdfs.length() - 1) : sourceDirInHdfs;
            }
        }

        return sourceDirInHdfs;
    }

    protected String getBaseSourceSchemaDir(TransformStep step, int baseSourceIdx) {
        Source baseSource = step.getBaseSources()[baseSourceIdx];
        if (!(baseSource instanceof DerivedSource)) {
            throw new IllegalArgumentException(
                    "Only source with DerivedSource type is supported to provide schema directory");
        }
        return hdfsPathBuilder.constructSchemaDir(baseSource.getSourceName(), step.getBaseVersions().get(baseSourceIdx))
                .toString();
    }

    protected String getBaseSourceVersionFilePath(TransformStep step, int baseSourceIdx) {
        Source baseSource = step.getBaseSources()[baseSourceIdx];
        if (!(baseSource instanceof DerivedSource || baseSource instanceof IngestionSource)) {
            throw new IllegalArgumentException(
                    "Only source with DerivedSource or IngestionSource type is supported to provide schema directory");
        }
        return hdfsPathBuilder.constructVersionFile(baseSource).toString();
    }

    @SuppressWarnings("unchecked")
    protected T getConfiguration(String confStr) {
        T configuration = null;
        Class<? extends TransformerConfig> configClass = getConfigurationClass();
        if (confStr == null) {
            if (configClass == TransformerConfig.class) {
                try {
                    configuration = (T) configClass.newInstance();
                } catch (Exception e) {
                    // ignored
                }
            }
        } else {
            try {
                configuration = (T) JsonUtils.deserialize(confStr, configClass);
            } catch (Exception e) {
                log.error("Failed to convert tranformer config.", e);
            }
        }
        return configuration;
    }

    @Override
    public boolean transform(TransformationProgress pipelineProgress, String workflowDir, TransformStep step) {
        try {
            log.info("Start in transformer " + getName());
            T configuration = getConfiguration(step.getConfig());
            if (configuration == null) {
                log.error("Invalid transformer configuration");
                updateStatusToFailed(pipelineProgress, "Failed to transform data.", null);
                return false;
            }
            return transformInternal(pipelineProgress, workflowDir, step);
        } catch (Exception e) {
            //TODO: investigate why it won't fail the whole workflow here.
            log.error("Transformer failed to transform", e);
            updateStatusToFailed(pipelineProgress, "Failed to transform data.", e);
            return false;
        }
    }

    protected void updateStatusToFailed(TransformationProgress progress, String errorMsg, Exception e) {
        progressHelper.updateStatusToFailed(progressEntityMgr, progress, errorMsg, e, log);
    }

    @Override
    public String outputSubDir() {
        return "";
    }

}
