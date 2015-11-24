package com.latticeengines.serviceflows.workflow.modeling;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("profileAndModel")
public class ProfileAndModel extends BaseWorkflowStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(ProfileAndModel.class);

    @Override
    public void execute() {
        log.info("Inside ProfileAndModel execute()");

        Table eventTable = JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);

        try {
            profileAndModel(eventTable);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28007, e, new String[] { eventTable.getName() });
        }
    }

    private void profileAndModel(Table eventTable) throws Exception {
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);

        String[] eventCols = new String[] { //
                "Event_IsWon", //
                "Event_StageIsClosedWon", //
                "Event_IsClosed", //
                "Event_OpportunityCreated" //
        };
        List<String> excludedColumns = new ArrayList<>();

        for (String eventCol : eventCols) {
            excludedColumns.add(eventCol);
        }

        for (Attribute attr : eventTable.getAttributes()) {
            if (attr.getApprovedUsage() == null || attr.getApprovedUsage().get(0).equals("None")) {
                excludedColumns.add(attr.getName());
            }
        }

        String[] excludeList = new String[excludedColumns.size()];
        excludedColumns.toArray(excludeList);
        bldr = bldr.profileExcludeList(excludeList);

        for (String eventCol : eventCols) {
            bldr = bldr.targets(eventCol) //
                    .metadataTable("EventTable-" + eventCol) //
                    .keyColumn("Id") //
                    .modelName("Model-" + eventCol);
            ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
            modelExecutor.writeMetadataFile();
            modelExecutor.profile();
            modelExecutor.model();
        }
    }

}
