package com.latticeengines.datacloud.etl.transformation.transformer.impl.am;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.am.MapAttributeTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MAP_ATTRIBUTE_TXFMR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroParquetUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.am.MapAttributeTxfmrConfig;
import com.latticeengines.spark.exposed.job.am.MapAttributeJob;

@Component(TRANSFORMER_NAME)
public class MapAttributeTxfmr extends ConfigurableSparkJobTxfmr<MapAttributeTxfmrConfig> {

    private static final Logger log = LoggerFactory.getLogger(MapAttributeTxfmr.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_MAP_ATTRIBUTE_TXFMR;

    public static final String DATA_CLOUD_VERSION = "DataCloudVersion";
    public static final String ACCOUNT_MASTER = "AccountMaster";
    public static final String MINI_PREFIX = "MINI_";

    public static final String MAP_STAGE = "MapStage";
    public static final String DERIVE_STAGE = "DeriveStage";
    public static final String REFRESH_STAGE = "RefreshStage";

    @Inject
    private DataCloudVersionService dataCloudVersionService;

    @Inject
    private TblDrivenUtils<MapAttributeTxfmrConfig, MapAttributeTxfmrConfig.MapFunc> tblDrivenUtils;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<MapAttributeJob> getSparkJobClz() {
        return MapAttributeJob.class;
    }

    @Override
    protected Class<MapAttributeTxfmrConfig> getJobConfigClz() {
        return MapAttributeTxfmrConfig.class;
    }

    @Override
    protected void preSparkJobProcessing(TransformStep step, String workflowDir, MapAttributeTxfmrConfig config) {
        if (!config.getStage().contains(REFRESH_STAGE)) {
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

        List<String> baseSourceNames = new ArrayList<>();
        for (Source src : baseSources) {
            baseSourceNames.add(src.getSourceName());
        }
        config.setBaseTables(baseSourceNames);
        config.setTemplates(baseSourceNames);
        Source targetTemplate = step.getTargetTemplate();
        if (targetTemplate != null) {
            config.setTimestampField(targetTemplate.getTimestampField());
        }
        List<SourceAttribute> srcAttrs = tblDrivenUtils.getAttributes(config, "MapAttribute");
        List<MapAttributeTxfmrConfig.MapFunc> mapFuncs = tblDrivenUtils.getAttributeFuncs(config, srcAttrs,
                MapAttributeTxfmrConfig.MapFunc.class);
        config.setSrcAttrs(srcAttrs);
        config.setMapFuncs(mapFuncs);

        log.info("MapAttributeTxfmrConfig=" + JsonUtils.serialize(config));

    }

    @Override
    protected Schema getTargetSchema(HdfsDataUnit result, MapAttributeTxfmrConfig config, //
            TransformerConfig configuration, List<Schema> baseSchemas) {

        if (!ACCOUNT_MASTER.equals(config.getSource())) {
            return null;
        }
        Schema resultSchema = AvroParquetUtils.parseAvroSchema(yarnConfiguration, result.getPath());
        if (StringUtils.isNotBlank(config.getDataCloudVersion())) {
            resultSchema.addProp(DATA_CLOUD_VERSION, config.getDataCloudVersion());
        } else {
            DataCloudVersion currentVersion = dataCloudVersionService.currentApprovedVersion();
            if (MAP_STAGE.equals(config.getStage())) {
                String version = dataCloudVersionService.nextMinorVersion(currentVersion.getVersion());
                if (Boolean.TRUE.equals(config.isMiniDataCloud())) {
                    version = MINI_PREFIX + version;
                }
                resultSchema.addProp(DATA_CLOUD_VERSION, version);
            } else if (REFRESH_STAGE.equals(config.getStage())) {
                String version = currentVersion.getVersion();
                if (Boolean.TRUE.equals(config.isMiniDataCloud())) {
                    version = MINI_PREFIX + version;
                }
                resultSchema.addProp(DATA_CLOUD_VERSION, version);
            }
        }
        return resultSchema;
    }

}
