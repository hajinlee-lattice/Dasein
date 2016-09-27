package com.latticeengines.propdata.engine.transformation.service.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMapping;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMappingImpl;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.dataflow.SingleBaseSourceRefreshDataFlowParameter;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.common.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component("dnbCacheSeedCleanDataFlowService")
public class DnBCacheSeedCleanDataFlowService extends AbstractTransformationDataFlowService {
    private static Logger LOG = LogManager.getLogger(DnBCacheSeedCleanDataFlowService.class);

    private static final String SOURCETABLES = "SOURCETABLES";

    private static final String SPLIT_REGEX = "\\|";

    private static final String HIPHEN = "-";

    private static final String FLOWNAME = "FLOWNAME";

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private SourceColumnEntityMgr sourceColumnEntityMgr;

    @Value("${propdata.collection.cascading.platform:tez}")
    private String cascadingPlatform;

    @SuppressWarnings("unchecked")
    @Override
    public void executeDataProcessing(Source source, String workflowDir, String baseVersion,
            String uid, String dataFlowBean,
            TransformationConfiguration transformationConfiguration) {
        if (StringUtils.isEmpty(dataFlowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }
        LOG.info("SourceName = " + source.getSourceName());
        if (baseVersion != null) {
            LOG.info("BaseVersion = " + baseVersion);
        }
        LOG.info("DataFlowBean = " + dataFlowBean);

        FixedIntervalSource fixedIntervalSource = (FixedIntervalSource) source;

        Map<String, Table> sources = new HashMap<>();
        List<String> baseTables = new ArrayList<>();
        List<List<SourceColumn>> baseSourceColumnsList = new ArrayList<>();
        List<List<String>> baseSourcePrimaryKeysList = new ArrayList<>();
        String[] versions = {};
        if (!StringUtils.isEmpty(baseVersion)) {
            versions = baseVersion.split(SPLIT_REGEX);
        }
        int i = 0;
        for (Source baseSource : fixedIntervalSource.getBaseSources()) {
            String version = baseVersion != null ? versions[i]
                    : hdfsSourceEntityMgr.getCurrentVersion(baseSource);
            LOG.info("baseSource = " + baseSource.getSourceName() + " version = " + version);
            Table baseTable = hdfsSourceEntityMgr.getTableAtVersion(baseSource, version);
            List<SourceColumn> baseSourceColumns = sourceColumnEntityMgr
                    .getSourceColumns(baseSource.getSourceName());
            baseSourceColumnsList.add(baseSourceColumns);
            CsvToAvroFieldMapping fieldMapping = new CsvToAvroFieldMappingImpl(baseSourceColumns);
            Schema schema = fieldMapping.getAvroSchema();
            baseTable.setSchema(schema);
            sources.put(baseSource.getSourceName(), baseTable);
            baseTables.add(baseSource.getSourceName());
            List<String> primaryKeys = Arrays.asList(baseSource.getPrimaryKey());
            baseSourcePrimaryKeysList.add(primaryKeys);
            i++;
        }

        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid)
                .toString();
        SingleBaseSourceRefreshDataFlowParameter parameters = new SingleBaseSourceRefreshDataFlowParameter();
        parameters.setTimestampField(source.getTimestampField());
        try {
            parameters.setTimestamp(HdfsPathBuilder.dateFormat.parse(baseVersion));
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25012, e,
                    new String[] { source.getSourceName(), e.getMessage() });
        }
        parameters.setColumns(
                sourceColumnEntityMgr.getSourceColumns(fixedIntervalSource.getSourceName()));
        parameters.setBaseSourceColumns(baseSourceColumnsList);
        parameters.setBaseTables(baseTables);
        parameters.setJoinFields(source.getPrimaryKey());
        parameters.setHasSqlPresence(source instanceof HasSqlPresence);

        DataFlowContext ctx = dataFlowContext(source, sources, parameters, targetPath);
        ctx.setProperty(FLOWNAME, source.getSourceName() + HIPHEN + flowName);
        ctx.setProperty(SOURCETABLES, sources);

        dataTransformationService.executeNamedTransformation(ctx, dataFlowBean);

    }

    @Override
    Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    @Override
    String getCascadingPlatform() {
        return cascadingPlatform;
    }
}
