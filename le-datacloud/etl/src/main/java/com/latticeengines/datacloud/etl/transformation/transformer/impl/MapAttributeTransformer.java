package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.MapAttributeFlow;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MapAttributeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(MapAttributeFlow.MAP_TRANSFORMER)
public class MapAttributeTransformer
        extends AbstractDataflowTransformer<MapAttributeConfig, TransformationFlowParameters> {

    private static final Logger log = LoggerFactory.getLogger(MapAttributeTransformer.class);

    public static final String DATA_CLOUD_VERSION = "DataCloudVersion";
    public static final String ACCOUNT_MASTER = "AccountMaster";
    public static final String MINI_PREFIX = "MINI_";

    @Autowired
    private DataCloudVersionService dataCloudVersionService;

    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir,
            TransformationFlowParameters parameters, MapAttributeConfig config) {
        if (!MapAttributeFlow.REFRESH_STAGE.equals(config.getStage())) {
            return;
        }
        Source[] baseSources = step.getBaseSources();
        int amIdx = -1;
        for (int i = 0; i < baseSources.length; i++) {
            if (ACCOUNT_MASTER.equals(baseSources[i].getSourceName())) {
                amIdx = i;
                break;
            }
        }
        if (amIdx == -1) {
            return;
        }
        String dataCloudVersion = config.getDataCloudVersion() != null ? config.getDataCloudVersion()
                : dataCloudVersionService.currentApprovedVersion().getVersion();
        List<String> versions = hdfsSourceEntityMgr.getVersions(baseSources[amIdx]);
        if (CollectionUtils.isEmpty(versions)) {
            throw new RuntimeException(
                    "Fail to find a version to refresh source " + baseSources[amIdx].getSourceName());
        }
        Collections.sort(versions, Collections.reverseOrder());
        String amVersion = null;
        for (String version : versions) {
            Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseSources[amIdx], version);
            if (schema == null) {
                throw new RuntimeException("Fail to find schema file for AccountMaster @" + version);
            }
            if (dataCloudVersion.equals(schema.getProp(MapAttributeTransformer.DATA_CLOUD_VERSION))) {
                amVersion = version;
                break;
            }
        }
        if (amVersion == null) {
            throw new RuntimeException(
                    "Fail to find a version of AccountMaster with DataCloudVersion " + dataCloudVersion);
        }
        log.info("Choose AccountMaster @" + amVersion + " as datacloud version " + dataCloudVersion + " to refresh");
        List<String> baseVersions = step.getBaseVersions();
        baseVersions.set(amIdx, amVersion);
    }

    @Override
    protected Schema getTargetSchema(Table result, TransformationFlowParameters parameters, MapAttributeConfig config,
            List<Schema> baseSchemas) {
        if (!ACCOUNT_MASTER.equals(config.getSource())) {
            return null;
        }
        String extractPath = result.getExtracts().get(0).getPath();
        String glob;
        if (extractPath.endsWith(".avro")) {
            glob = extractPath;
        } else if (extractPath.endsWith(File.pathSeparator)) {
            glob = extractPath + "*.avro";
        } else {
            glob = extractPath + File.separator + "*.avro";
        }
        Schema parsed = AvroUtils.getSchemaFromGlob(yarnConfiguration, glob);
        if (StringUtils.isNotBlank(config.getDataCloudVersion())) {
            parsed.addProp(DATA_CLOUD_VERSION, config.getDataCloudVersion());
        } else {
            DataCloudVersion currentVersion = dataCloudVersionService.currentApprovedVersion();
            if (MapAttributeFlow.MAP_STAGE.equals(config.getStage())) {
                String version = dataCloudVersionService.nextMinorVersion(currentVersion.getVersion());
                if (Boolean.TRUE.equals(config.isMiniDataCloud())) {
                    version = MINI_PREFIX + version;
                }
                parsed.addProp(DATA_CLOUD_VERSION, version);
            } else if (MapAttributeFlow.REFRESH_STAGE.equals(config.getStage())) {
                String version = currentVersion.getVersion();
                if (Boolean.TRUE.equals(config.isMiniDataCloud())) {
                    version = MINI_PREFIX + version;
                }
                parsed.addProp(DATA_CLOUD_VERSION, version);
            }
        }
        return parsed;
    }

    @Override
    protected String getDataFlowBeanName() {
        return MapAttributeFlow.BEAN_NAME;
    }

    @Override
    public String getName() {
        return MapAttributeFlow.MAP_TRANSFORMER;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return MapAttributeConfig.class;
    }

}
