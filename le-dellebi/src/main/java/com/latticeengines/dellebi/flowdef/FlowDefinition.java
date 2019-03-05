package com.latticeengines.dellebi.flowdef;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.PipeFactory;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

@Component
public class FlowDefinition {

    private static final Logger log = LoggerFactory.getLogger(FlowDefinition.class);

    @Value("${dellebi.cascadinginputdelimiter}")
    private String cascadingInputDelimiter;

    @Inject
    private DellEbiFlowService dellEbiFlowService;

    @Inject
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    @SuppressWarnings("rawtypes")
    public FlowDef populateFlowDefByType(String type) {

        log.info("Initial " + type + " flow definition!");
        DataFlowContext context = new DataFlowContext();
        context.setProperty(DellEbiFlowService.FILE_TYPE, type);
        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter),
                dellEbiFlowService.getTxtDir(context));
        Tap outTapFile = new Hfs(new TextDelimited(false, "\t"), dellEbiFlowService.getOutputDir(context),
                SinkMode.UPDATE);
        Tap failedRowsTapFile = new Hfs(new TextDelimited(false, "\t"), dellEbiFlowService.getErrorOutputDir(null),
                SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;

        String inputFields = dellEbiConfigEntityMgr.getInputFields(type);
        String exportedFields = dellEbiConfigEntityMgr.getOutputFields(type);

        try {
            filePipe = PipeFactory.getPipe("generic_item_Pipe", inputFields, exportedFields);
        } catch (Exception e) {
            log.error("Failed to get " + type + "  pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile).addTailSink(filePipe,
                outTapFile);
        flowDef_fileType.addTrap(filePipe, failedRowsTapFile);
        flowDef_fileType.setName(type);

        return flowDef_fileType;
    }
}
