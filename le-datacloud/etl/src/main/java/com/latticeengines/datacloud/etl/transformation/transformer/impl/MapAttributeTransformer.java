package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.dataflow.transformation.MapAttributeFlow;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MapAttributeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(MapAttributeFlow.MAP_TRANSFORMER)
public class MapAttributeTransformer
        extends AbstractDataflowTransformer<MapAttributeConfig, TransformationFlowParameters> {

    public static final String DATA_CLOUD_VERSION = "DataCloudVersion";
    public static final String ACCOUNT_MASTER = "AccountMaster";

    @Autowired
    private DataCloudVersionService dataCloudVersionService;

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
                String nextVersion = dataCloudVersionService.nextMinorVersion(currentVersion.getVersion());
                parsed.addProp(DATA_CLOUD_VERSION, nextVersion);
            } else if (MapAttributeFlow.REFRESH_STAGE.equals(config.getStage())) {
                parsed.addProp(DATA_CLOUD_VERSION, currentVersion.getVersion());
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
