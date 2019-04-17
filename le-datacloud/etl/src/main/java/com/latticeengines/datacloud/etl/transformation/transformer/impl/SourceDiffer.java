package com.latticeengines.datacloud.etl.transformation.transformer.impl;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.dataflow.transformation.Diff;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiffferParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DifferConfig;
import com.latticeengines.domain.exposed.metadata.Table;


@Component(Diff.TRANSFORMER_NAME)
public class SourceDiffer extends AbstractDataflowTransformer<DifferConfig, DiffferParameters> {

    private static final Logger log = LoggerFactory.getLogger(SourceDiffer.class);

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private DataCloudVersionService dataCloudVersionService;

    @Override
    protected String getDataFlowBeanName() {
        return Diff.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return Diff.TRANSFORMER_NAME;
    }

    @Override
    protected Class<DiffferParameters> getDataFlowParametersClass() {
        return DiffferParameters.class;
    }

    @Override
    protected Class<? extends DifferConfig> getConfigurationClass() {
        return DifferConfig.class;
    }

    @Override
    protected boolean validateConfig(DifferConfig config, List<String> sourceNames) {
        String error;
        if (sourceNames.size() != 1 && sourceNames.size() != 2) {
            error = "Only support diff one source with 2 different versions or two sources";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        if ((StringUtils.isBlank(config.getDiffVersion()) && StringUtils.isNotBlank(config.getDiffVersionCompared()))
                || (StringUtils.isNotBlank(config.getDiffVersion())
                        && StringUtils.isBlank(config.getDiffVersionCompared()))) {
            error = "Either provide both DiffVersion and DiffVersionComparedTo or by default use latest version as DiffVersion and second to latest version as DiffVersionCompared";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        if (StringUtils.isNotBlank(config.getDiffVersion()) && sourceNames.size() != 1) {
            error = "If diff one source with different versions, only support one base source";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        if (config.getKeys() == null || config.getKeys().length == 0) {
            error = "Please provide primary key fields";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        return true;
    }

    @Override
    protected void updateParameters(DiffferParameters parameters, Source[] baseSource, Source target,
            DifferConfig config, List<String> baseVersions) {
        if (StringUtils.isBlank(config.getDiffVersion()) && baseSource.length == 1) {
            List<String> versions = hdfsSourceEntityMgr.getVersions(baseSource[0]);
            if (versions == null || versions.size() < 2) {
                throw new RuntimeException("No enough versions to diff for source " + baseSource[0].getSourceName());
            }
            Collections.sort(versions, Collections.reverseOrder());
            if (baseSource[0].getSourceName().equals(MapAttributeTransformer.ACCOUNT_MASTER)) {
                String dataCloudVersion = config.getDataCloudVersion() != null ? config.getDataCloudVersion()
                        : dataCloudVersionService.currentApprovedVersion().getVersion();
                log.info("Diff AccountMaster of datacloud version " + dataCloudVersion);
                for (String version : versions) {
                    Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseSource[0], version);
                    if (schema == null) {
                        throw new RuntimeException("Fail to find schema file for AccountMaster @" + version);
                    }
                    if (dataCloudVersion.equals(schema.getProp(MapAttributeTransformer.DATA_CLOUD_VERSION))) {
                        if (parameters.getDiffVersion() == null) {
                            parameters.setDiffVersion(version);
                        } else if (parameters.getDiffVersionCompared() == null) {
                            parameters.setDiffVersionCompared(version);
                        }
                    }
                    if (parameters.getDiffVersion() != null && parameters.getDiffVersionCompared() != null) {
                        break;
                    }
                }
                if (parameters.getDiffVersion() == null || parameters.getDiffVersionCompared() == null) {
                    throw new RuntimeException(
                            "Fail to find two versions of AccountMaster with DataCloudVersion " + dataCloudVersion);
                }
            } else {
                parameters.setDiffVersion(versions.get(0));
                parameters.setDiffVersionCompared(versions.get(1));
            }
        } else {
            parameters.setDiffVersion(config.getDiffVersion());
            parameters.setDiffVersionCompared(config.getDiffVersionCompared());
        }
        if (baseSource.length == 1) {
            log.info(String.format("Diff source %s between version %s and %s", baseSource[0].getSourceName(),
                    parameters.getDiffVersion(), parameters.getDiffVersionCompared()));
        }
        parameters.setExcludeFields(config.getExcludeFields());
        parameters.setKeys(config.getKeys());
    }

    @Override
    protected Map<Source, List<String>> setupBaseSourceVersionMap(TransformStep step, DiffferParameters parameters,
            DifferConfig config) {
        Map<Source, List<String>> baseSourceVersionMap = new HashMap<Source, List<String>>();
        if (step.getBaseSources().length == 1) {
            List<String> versionList = new ArrayList<>();
            versionList.add(parameters.getDiffVersion());
            versionList.add(parameters.getDiffVersionCompared());
            baseSourceVersionMap.put(step.getBaseSources()[0], versionList);
        } else {
            for (int i = 0; i < step.getBaseSources().length; i++) {
                Source baseSource = step.getBaseSources()[i];
                List<String> versionList = new ArrayList<>();
                versionList.add(step.getBaseVersions().get(i));
                baseSourceVersionMap.put(baseSource, versionList);
            }
        }
        return baseSourceVersionMap;
    }

    @Override
    protected boolean addSource(Map<String, Table> sourceTables, Source source, List<String> versions) {
        String sourceName = source.getSourceName();
        try {
            if (source instanceof TableSource) {
                TableSource tableSource = (TableSource) source;
                Table sourceTable = metadataProxy.getTable(tableSource.getCustomerSpace().toString(),
                        tableSource.getTable().getName());
                sourceTables.put(sourceName, sourceTable);
                log.info("Select table source " + sourceName);
                return true;
            }
            for (String version : versions) {
                Table sourceTable = hdfsSourceEntityMgr.getTableAtVersion(source, version);
                log.info("Select source " + sourceName + " @version " + version);
                sourceTables.put(versions.size() == 1 ? sourceName : Diff.getTableName(sourceName, version),
                        sourceTable);
            }
            return true;
        } catch (Exception e) {
            log.error("Source " + sourceName + " is not initiated in HDFS", e);
            return false;
        }
    }
}
