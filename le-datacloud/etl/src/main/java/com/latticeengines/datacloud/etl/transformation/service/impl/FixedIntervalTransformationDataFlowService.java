package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.FixedIntervalSource;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMapping;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMappingImpl;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.dataflow.DepivotDataFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("fixedIntervalTransformationDataFlowService")
public class FixedIntervalTransformationDataFlowService extends AbstractTransformationDataFlowService {

    public void executeDataProcessing(Source source, String workflowDir, String baseVersion, String uid,
                                      String dataFlowBean, TransformationConfiguration transformationConfiguration) {
        if (StringUtils.isEmpty(dataFlowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }

        FixedIntervalSource fixedIntervalSource = (FixedIntervalSource) source;

        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        Map<String, Table> sources = new HashMap<>();
        List<String> baseTables = new ArrayList<>();
        List<List<SourceColumn>> baseSourceColumnsList = new ArrayList<>();
        String[] versions = baseVersion.split(SPLIT_REGEX);
        int i = 0;
        for (Source baseSource : fixedIntervalSource.getBaseSources()) {
            String version = versions[i];
            Table baseTable = hdfsSourceEntityMgr.getTableAtVersion(baseSource, version);
            List<SourceColumn> baseSourceColumns = sourceColumnEntityMgr.getSourceColumns(baseSource.getSourceName());
            baseSourceColumnsList.add(baseSourceColumns);
            CsvToAvroFieldMapping fieldMapping = new CsvToAvroFieldMappingImpl(baseSourceColumns);
            Schema schema = fieldMapping.getAvroSchema();
            baseTable.setSchema(schema);
            sources.put(baseSource.getSourceName(), baseTable);
            baseTables.add(baseSource.getSourceName());
            i++;
        }

        DepivotDataFlowParameters parameters = new DepivotDataFlowParameters();
        parameters.setTimestampField(source.getTimestampField());
        try {
            parameters.setTimestamp(HdfsPathBuilder.dateFormat.parse(baseVersion));
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25012, e, new String[] { source.getSourceName(), e.getMessage() });
        }
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(fixedIntervalSource.getSourceName()));
        parameters.setBaseSourceColumns(baseSourceColumnsList);
        parameters.setBaseTables(baseTables);
        parameters.setJoinFields(source.getPrimaryKey());
        parameters.setHasSqlPresence(source instanceof HasSqlPresence);

        DataFlowContext ctx = dataFlowContext(source, sources, parameters, targetPath);
        ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + HIPHEN + flowName);
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sources);

        dataTransformationService.executeNamedTransformation(ctx, dataFlowBean);
    }

}
