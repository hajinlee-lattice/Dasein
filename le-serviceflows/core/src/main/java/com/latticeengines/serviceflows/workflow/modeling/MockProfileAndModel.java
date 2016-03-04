package com.latticeengines.serviceflows.workflow.modeling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("mockProfileAndModel")
public class MockProfileAndModel extends BaseWorkflowStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(MockProfileAndModel.class);
    private static final String MODEL_HDFS_BASEDIR = "/user/s-analytics/customers/%s/models/RunMatchWithLEUniverse_123_DerivedColumns/";
    private static final String MODEL_SOURCEDIR = "/tmp/PDEndToEndTest/models/";

    @Override
    public void execute() {
        log.info("Inside MockProfileAndModel execute()");
        Table eventTable = JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);

        Map<String, String> modelApplicationIdToEventColumn;
        try {
            modelApplicationIdToEventColumn = profileAndModel(eventTable);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28007, e, new String[] { eventTable.getName() });
        }

        executionContext.putString(MODEL_APP_IDS, JsonUtils.serialize(modelApplicationIdToEventColumn));

    }

    private Map<String, String> profileAndModel(Table eventTable) throws Exception {
        Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);

        uploadOrOverrideExistingModels(bldr, modelApplicationIdToEventColumn);

        List<String> excludedColumns = new ArrayList<>();
        excludedColumns.add("Event_OpportunityCreated");

        for (Attribute attr : eventTable.getAttributes()) {
            if (attr.getApprovedUsage() == null || attr.getApprovedUsage().get(0).equals("None")) {
                excludedColumns.add(attr.getName());
            }
        }

        String[] excludeList = new String[excludedColumns.size()];
        excludedColumns.toArray(excludeList);
        bldr = bldr.profileExcludeList(excludeList);

        String eventCol = "Event_OpportunityCreated";
        bldr = bldr.targets(eventCol) //
                .metadataTable("EventTable-" + eventCol) //
                .keyColumn("Id") //
                .modelName("Model-" + eventCol);
        ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
        modelExecutor.writeMetadataFiles();
        modelExecutor.profile();
        String modelAppId = modelExecutor.model();
        log.info(String.format("The generated model app id is: %s", modelAppId));
        modelApplicationIdToEventColumn.put(modelAppId, eventCol);

        return modelApplicationIdToEventColumn;
    }

    private void uploadOrOverrideExistingModels(ModelingServiceExecutor.Builder bldr,
            Map<String, String> modelApplicationIdToEventColumn) throws Exception {
        FileSystem fs = FileSystem.get(bldr.getYarnConfiguration());
        fs.delete(new Path(String.format(MODEL_HDFS_BASEDIR, configuration.getCustomerSpace().toString())), true);
        fs.mkdirs(new Path(String.format(MODEL_HDFS_BASEDIR, configuration.getCustomerSpace().toString())));
        log.info(String.format(MODEL_HDFS_BASEDIR, configuration.getCustomerSpace().toString()) + "created");

        FileStatus[] fileStatuses = fs.listStatus(new Path(MODEL_SOURCEDIR));
        Path modelBaseDir = new Path(String.format(MODEL_HDFS_BASEDIR, configuration.getCustomerSpace().toString()));
        for (FileStatus fileStatus : fileStatuses) {
            Path fullEventColumnPath = fileStatus.getPath();
            String eventColumnName = fullEventColumnPath.toString().substring(
                    fullEventColumnPath.toString().lastIndexOf("/") + 1);

            Path fullModelPath = fs.listStatus(fullEventColumnPath)[0].getPath();
            String modelName = fullModelPath.toString().substring(fullModelPath.toString().lastIndexOf("/"));

            String applicationIdPath = fs.listStatus(fullModelPath)[0].getPath().toString();
            String applicationId = String.format("application_%s",
                    applicationIdPath.substring(applicationIdPath.lastIndexOf("/") + 1));
            fs.setTimes(new Path(applicationIdPath + "/enhancements/modelsummary.json"), System.currentTimeMillis(),
                    System.currentTimeMillis());

            boolean renameSucc = fs.rename(fullModelPath, new Path(modelBaseDir.toString() + modelName));
            modelApplicationIdToEventColumn.put(applicationId, eventColumnName);
        }
    }

}
