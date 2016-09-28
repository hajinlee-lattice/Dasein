package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.source.Source;

@Component("simpleTransformationDataFlowService")
public class SimpleTransformationDataFlowService extends AbstractTransformationDataFlowService {

    private static final Log log = LogFactory.getLog(SimpleTransformationDataFlowService.class);

    public void executeDataFlow(Source source, String workflowDir, Map<Source, String> baseSourceVersions,
                                String flowBean, TransformationFlowParameters parameters) {

        if (StringUtils.isEmpty(flowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }

        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        Map<String, Table> sourceTables = setupSourceTables(baseSourceVersions);

        DataFlowContext ctx = dataFlowContext(source, sourceTables, parameters, workflowDir);
        ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + HIPHEN + flowName);
        dataTransformationService.executeNamedTransformation(ctx, flowBean);
    }

    private Map<String, Table> setupSourceTables(Map<Source, String> baseSourceVersions) {
        Map<String, Table> sourceTables = new HashMap<>();
        for (Map.Entry<Source, String> entry: baseSourceVersions.entrySet()) {
            Source baseSource = entry.getKey();
            String baseSourceVersion = entry.getValue();
            log.info("Add base source " + baseSource.getSourceName());
            addSource(sourceTables, baseSource, baseSourceVersion);
        }
        return sourceTables;
    }


    private boolean addSource(Map<String, Table> sourceTables, Source source, String version) {
        String sourceName = source.getSourceName();
        Table sourceTable = null;
        try {
            sourceTable = hdfsSourceEntityMgr.getTableAtVersion(source, version);
            log.info("Select source " + sourceName + "@version " + version);
        } catch (Exception e) {
            log.info("Source " + sourceName + " is not initiated in HDFS");
            e.printStackTrace();
            return false;
        }
        sourceTables.put(sourceName, sourceTable);
        return true;
    }

}
