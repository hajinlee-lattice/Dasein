package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.dataflow.DepivotDataFlowParameters;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.common.entitymgr.SourceColumnEntityMgr;

@Component("fixedIntervalTransformationDataFlowService")
public class FixedIntervalTransformationDataFlowService extends AbstractTransformationDataFlowService {

    private static final String SOURCETABLES = "SOURCETABLES";

    private static final String SCHEMA_BOMBORA_FIREHOSE_AVRO = "schema/BomboraFirehoseAvroSchema.avsc";

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

    @Override
    public void executeDataProcessing(Source source, String workflowDir, String baseVersion, String uid,
            String dataFlowBean) {
        if (StringUtils.isEmpty(dataFlowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }

        FixedIntervalSource fixedIntervalSource = (FixedIntervalSource) source;

        String flowName = CollectionDataFlowKeys.DEPIVOT_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        Map<String, Table> sources = new HashMap<>();
        List<String> baseTables = new ArrayList<>();
        String[] versions = baseVersion.split(SPLIT_REGEX);
        int i = 0;
        for (Source baseSource : fixedIntervalSource.getBaseSources()) {
            String version = versions[i];
            Table baseTable = hdfsSourceEntityMgr.getTableAtVersion(baseSource, version);
            try (InputStream schemaContent = this.getClass().getClassLoader()
                    .getResourceAsStream(SCHEMA_BOMBORA_FIREHOSE_AVRO)) {
                Schema schema = new Schema.Parser().parse(schemaContent);
                baseTable.setSchema(schema);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25012, e,
                        new String[] { source.getSourceName(), e.getMessage() });
            }
            sources.put(baseSource.getSourceName(), baseTable);
            baseTables.add(baseSource.getSourceName());
            i++;
        }

        DepivotDataFlowParameters parameters = new DepivotDataFlowParameters();
        parameters.setTimestampField(source.getTimestampField());
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(fixedIntervalSource));
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
