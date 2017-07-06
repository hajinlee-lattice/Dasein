package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.RefreshedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

/**
 * This is use to submit basic dataflows. It reads Map<Source, String>
 * baseSourceVersions to add source tables. then submit data flow using passed
 * in <P extends TransformationFlowParameters>
 */
@Component("simpleTransformationDataFlowService")
public class SimpleTransformationDataFlowService extends AbstractTransformationDataFlowService {

    private static final Log log = LogFactory.getLog(SimpleTransformationDataFlowService.class);

    @Autowired
    private MetadataProxy metadataProxy;

    public <P extends TransformationFlowParameters> Table executeDataFlow(TransformStep step, String flowBean,
            P parameters, String workflowDir) {
        if (StringUtils.isEmpty(flowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { step.getTargetTemplate().getSourceName(), "Name of dataFlowBean cannot be null" });
        }
        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        DataFlowContext ctx = dataFlowContext(step.getTargetTemplate(), step.getBaseTables(), parameters, workflowDir);
        if (step.getTargetTemplate() instanceof PipelineSource) {
            ctx.setProperty(DataFlowProperty.FLOWNAME, step.getTargetTemplate().getSourceName() + HIPHEN + flowName);
        } else {
            ctx.setProperty(DataFlowProperty.FLOWNAME, step.getTargetTemplate().getSourceName() + HIPHEN + flowBean);
        }
        return dataTransformationService.executeNamedTransformation(ctx, flowBean);
    }

    @Deprecated
    public <P extends TransformationFlowParameters> Table executeDataFlow(Source source, String workflowDir,
            Map<Source, List<String>> baseSourceVersions, String flowBean, P parameters) {

        if (StringUtils.isEmpty(flowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }

        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        Map<String, Table> sourceTables = setupSourceTables(baseSourceVersions);

        if (source instanceof RefreshedSource) {
            try {
                String currentVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
                sourceTables.put(source.getSourceName(), hdfsSourceEntityMgr.getTableAtVersion(source, currentVersion));
                log.info("Select source " + source.getSourceName() + "@versions " + currentVersion);
            } catch (Exception e) {
                log.info("Source " + source.getSourceName() + " is not initiated in HDFS");
                e.printStackTrace();
            }
        }

        DataFlowContext ctx = dataFlowContext(source, sourceTables, parameters, workflowDir);
        if (source instanceof PipelineSource) {
            ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + HIPHEN + flowName);
        } else {
            ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + HIPHEN + flowBean);
        }
        return dataTransformationService.executeNamedTransformation(ctx, flowBean);
    }

    @Deprecated
    private Map<String, Table> setupSourceTables(Map<Source, List<String>> baseSourceVersions) {
        Map<String, Table> sourceTables = new HashMap<>();
        for (Map.Entry<Source, List<String>> entry : baseSourceVersions.entrySet()) {
            Source baseSource = entry.getKey();
            List<String> baseSourceVersion = entry.getValue();
            log.info("Add base source " + baseSource.getSourceName());
            addSource(sourceTables, baseSource, baseSourceVersion);
        }
        return sourceTables;
    }

    @Deprecated
    private boolean addSource(Map<String, Table> sourceTables, Source source, List<String> versions) {
        String sourceName = source.getSourceName();
        Table sourceTable;
        try {
            if (source instanceof TableSource) {
                TableSource tableSource = (TableSource) source;
                sourceTable = metadataProxy.getTable(tableSource.getCustomerSpace().toString(), tableSource.getTable().getName());
            } else if (versions.size() == 1) {
                sourceTable = hdfsSourceEntityMgr.getTableAtVersion(source, versions.get(0));
            } else {
                sourceTable = hdfsSourceEntityMgr.getTableAtVersions(source, versions);
            }
            log.info("Select source " + sourceName + "@versions " + StringUtils.join(versions, ","));
        } catch (Exception e) {
            log.info("Source " + sourceName + " is not initiated in HDFS");
            e.printStackTrace();
            return false;
        }
        sourceTables.put(sourceName, sourceTable);
        return true;
    }

}
