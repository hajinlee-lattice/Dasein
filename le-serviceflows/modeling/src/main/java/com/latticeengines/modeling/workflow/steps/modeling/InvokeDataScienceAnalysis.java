package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.JobPublisher;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.DataScienceInvocationInfo;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

@Component("invokeDataScienceAnalysis")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class InvokeDataScienceAnalysis extends BaseModelStep<ModelStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(InvokeDataScienceAnalysis.class);

    public static final String DataScienceWorkflowPath = "/Workflow/Modeling/DataScience";

    private ModelProxy proxy = null;

    @Value("${hadoop.fs.web.defaultFS}")
    private String hdfsWebFS;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    public DataScienceInvocationInfo findInfo() {
        ModelSummary summary = modelSummaryProxy.getModelSummaryFromModelId(configuration.getCustomerSpace().toString(),
                configuration.getSourceModelSummary().getId());

        DataScienceInvocationInfo info = new DataScienceInvocationInfo();
        info.setModelApplicationID(summary.getApplicationId());
        info.setModelLookupID(summary.getLookupId());

        String[] parts = summary.getLookupId().split("|");

        Path extractsDir = new Path(configuration.getModelingServiceHdfsBaseDir())
                .append(configuration.getCustomerSpace().toString()).append("data").append(summary.getEventTableName())
                .append("samples");

        info.setExtractsDirectory(extractsDir.toString());

        Path modelsDir = new Path(configuration.getModelingServiceHdfsBaseDir())
                .append(configuration.getCustomerSpace().toString()).append("models").append(parts[1]).append(parts[2])
                .append(ApplicationIdUtils.stripJobId(summary.getApplicationId()));

        info.setModelDirectory(modelsDir.toString());
        return info;
    }

    public DataScienceInvocationInfo findInfo_HDFS(String customer, String baseCustomerFolder, String eventFolderName) {
        String trainingFileName = "allTraining-r-00000\\.avro";
        String modelJsonPattern = ".*_model.csv";
        String modelsRootDirectory = baseCustomerFolder + "/models/" + eventFolderName;
        String dataRootDirectory = baseCustomerFolder + "/data/" + eventFolderName;

        DataScienceInvocationInfo toReturn = new DataScienceInvocationInfo();
        List<String> avroSamples = null;
        try {
            avroSamples = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, dataRootDirectory, trainingFileName,
                    true);
        } catch (Exception exc) {
            log.error("Could not look up Training Avro File", exc);
        }

        if ((avroSamples == null) || (avroSamples.size() == 0)) {
            throw new LedpException(LedpCode.LEDP_10011, new Object[] { trainingFileName });
        }

        toReturn.setExtractsDirectory(
                avroSamples.get(0).replace("/allTraining-r-00000.avro", "").replace(hdfsWebFS, ""));

        List<String> models = null;
        try {
            models = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, modelsRootDirectory, modelJsonPattern, true);
        } catch (Exception exc) {
            log.error("Could not look up Model.csv File", exc);
        }

        if ((models == null) || (models.size() == 0)) {
            throw new LedpException(LedpCode.LEDP_10011, new Object[] { modelJsonPattern });
        }

        String fullyQualified = models.get(0);
        String[] parts = fullyQualified.split("/");
        String fileName = parts[parts.length - 1];

        String modelID = fileName.replace("_model.csv", "");
        String modelsDirectory = fullyQualified.replace("/" + fileName, "").replace(hdfsWebFS, "");

        toReturn.setModelId(modelID);
        toReturn.setModelDirectory(modelsDirectory);

        return toReturn;
    }

    @Override
    public void execute() {
        try {
            log.info("Executing InvokeDataScienceAnalysis.execute");

            if (proxy == null) {
                proxy = new ModelProxy();
            }

            Table eventTable = getEventTable();
            List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
            for (Attribute event : events) {
                try {
                    log.info("Populating HDFS Model Path information for model: " + configuration.getModelName());

                    String customer = configuration.getCustomerSpace().getContractId();
                    String currentEventFolderName = getEventFolderName(eventTable, event);
                    String baseCustomerDirectory = "";
                    if (configuration.getModelingServiceHdfsBaseDir().endsWith("/")) {
                        baseCustomerDirectory = String.format("%s%s", configuration.getModelingServiceHdfsBaseDir(),
                                configuration.getCustomerSpace().toString());
                    } else {
                        baseCustomerDirectory = String.format("%s/%s", configuration.getModelingServiceHdfsBaseDir(),
                                configuration.getCustomerSpace().toString());
                    }

                    // This code should perhaps be moved into the modelExecutor
                    // and Builder.
                    DataScienceInvocationInfo info = null;
                    try {
                        info = findInfo_HDFS(customer, baseCustomerDirectory, currentEventFolderName);
                    } catch (Exception exc) {
                        log.info("Could not use default ModelSummary lookup due to error: " + exc.getMessage());
                    }

                    if (info == null) {
                        info = findInfo();
                    }

                    info.setModelLookupID(configuration.getSourceModelSummary().getLookupId());

                    if (info.getModelApplicationID() == null)
                        info.setModelApplicationID(configuration.getSourceModelSummary().getApplicationId());

                    info.setCustomer(configuration.getCustomerSpace().getContractId());
                    info.setCustomerSpace(configuration.getCustomerSpace().toString());
                    info.setUserId(configuration.getUserName());
                    info.setStorageProtocol("HDFS");
                    info.setHdfsWebURL(hdfsWebFS);

                    AttributeMap fileMap = new AttributeMap();

                    // Fill in files that may be inferred here.

                    info.setFileTypeMap(fileMap);

                    JobPublisher publisher = new JobPublisher(DataScienceWorkflowPath);
                    String dsJobID = "ModelQuality_" + configuration.getSourceModelSummary().getId();
                    publisher.registerJob(dsJobID, JsonUtils.serialize(info));
                } catch (LedpException e) {
                    log.info("Failed to execute individual model registration, continuing: " + e.getMessage());
                }
            }
        } catch (Exception exc) {
            log.info("Failed to invoke Data Science Analyis: " + exc.getMessage());
        }
    }
}
