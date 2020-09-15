package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.GenerateRecommendationCSVContext;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateRecommendationCSVConfig;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateRecommendationCSVJob;

@Component("deltaCampaignLaunchInitStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeltaCampaignLaunchExportFileGeneratorStep
        extends RunSparkJob<DeltaCampaignLaunchExportFilesGeneratorConfiguration, GenerateRecommendationCSVConfig> {
    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchExportFileGeneratorStep.class);

    private boolean createAddCsvDataFrame;

    private boolean createDeleteCsvDataFrame;

    @Override
    protected Class<GenerateRecommendationCSVJob> getJobClz() {
        return GenerateRecommendationCSVJob.class;
    }

    @Override
    protected GenerateRecommendationCSVConfig configureJob(DeltaCampaignLaunchExportFilesGeneratorConfiguration stepConfiguration) {
        createAddCsvDataFrame = Boolean.toString(true)
                .equals(getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        createDeleteCsvDataFrame = Boolean.toString(true).equals(
                getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));
        log.info("createAddCsvDataFrame=" + createAddCsvDataFrame + ", createDeleteCsvDataFrame="
                + createDeleteCsvDataFrame);
        GenerateRecommendationCSVConfig generateRecommendationCSVConfig = new GenerateRecommendationCSVConfig();
        GenerateRecommendationCSVContext generateRecommendationCSVContext = new GenerateRecommendationCSVContext();
        generateRecommendationCSVConfig.setGenerateRecommendationCSVContext(generateRecommendationCSVContext);
        int target = 0;
        List<DataUnit> inputs = new ArrayList<>();
        if (createAddCsvDataFrame) {
            target++;
            createDateUnit(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH, inputs);
        }
        if (createDeleteCsvDataFrame) {
            target++;
            createDateUnit(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH, inputs);
        }
        generateRecommendationCSVConfig.setTargetNums(target);
        generateRecommendationCSVConfig.setInput(inputs);
        return generateRecommendationCSVConfig;
    }

    private void createDateUnit(String contextKey, List<DataUnit> inputs) {
        String path = getStringValueFromContext(contextKey);
        DataUnit dataUnit = HdfsDataUnit.fromPath(path);
        inputs.add(dataUnit);
    }

    @Override
    protected CustomerSpace parseCustomerSpace(DeltaCampaignLaunchExportFilesGeneratorConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        if (createAddCsvDataFrame && createDeleteCsvDataFrame) {
            putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_FILES, Lists.newArrayList(result.getTargets().get(0).getPath()));
            putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_FILES, Lists.newArrayList(result.getTargets().get(1).getPath()));
        } else if (createAddCsvDataFrame) {
            putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_FILES, Lists.newArrayList(result.getTargets().get(0).getPath()));
        } else {
            putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_FILES, Lists.newArrayList(result.getTargets().get(0).getPath()));
        }
    }
}
