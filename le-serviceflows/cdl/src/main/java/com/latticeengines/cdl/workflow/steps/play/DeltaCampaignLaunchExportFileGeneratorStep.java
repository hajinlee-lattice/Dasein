package com.latticeengines.cdl.workflow.steps.play;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.GenerateRecommendationCSVContext;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateRecommendationCSVConfig;
import com.latticeengines.domain.exposed.util.ChannelConfigUtil;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateRecommendationCSVJob;

@Component("deltaCampaignLaunchExportFileGeneratorStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeltaCampaignLaunchExportFileGeneratorStep
        extends RunSparkJob<DeltaCampaignLaunchExportFilesGeneratorConfiguration, GenerateRecommendationCSVConfig> {
    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchExportFileGeneratorStep.class);

    private boolean createAddCsvDataFrame;

    private boolean createDeleteCsvDataFrame;

    private static final String ADD_FILE_PREFIX = "add";

    private static final String DELETE_FILE_PREFIX = "delete";

    @Override
    protected Class<GenerateRecommendationCSVJob> getJobClz() {
        return GenerateRecommendationCSVJob.class;
    }

    @Override
    protected GenerateRecommendationCSVConfig configureJob(DeltaCampaignLaunchExportFilesGeneratorConfiguration config) {
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
        Map<String, String> accountDisplayNames = getMapObjectFromContext(RECOMMENDATION_ACCOUNT_DISPLAY_NAMES,
                String.class, String.class);
        Map<String, String> contactDisplayNames = getMapObjectFromContext(RECOMMENDATION_CONTACT_DISPLAY_NAMES,
                String.class, String.class);
        log.info("accountDisplayNames map: " + accountDisplayNames);
        log.info("contactDisplayNames map: " + contactDisplayNames);
        if (accountDisplayNames != null) {
            config.setAccountDisplayNames(accountDisplayNames);
        }
        if (contactDisplayNames != null) {
            config.setContactDisplayNames(contactDisplayNames);
        }
        generateRecommendationCSVContext.setIgnoreAccountsWithoutContacts(shouldIgnoreAccountsWithoutContacts(config));
        boolean isLiveRamp = CDLExternalSystemName.LIVERAMP.contains(config.getDestinationSysName());
        buildFieldsAndDisplayNames(generateRecommendationCSVContext, config.getAccountDisplayNames(), config.getContactDisplayNames(), isLiveRamp);
        return generateRecommendationCSVConfig;
    }

    private void buildFieldsAndDisplayNames(GenerateRecommendationCSVContext generateRecommendationCSVContext, Map<String, String> accountDisplayNames,
                                            Map<String, String> contactDisplayNames, boolean isLiveRamp) {
        Map<String, String> displayNames;
        List<String> fields;
        if (isLiveRamp) {
            displayNames = MapUtils.isNotEmpty(contactDisplayNames) ? contactDisplayNames : new HashMap<>();
            fields = new ArrayList<>(contactDisplayNames.keySet());
            Collections.sort(fields);
        } else {
            displayNames = MapUtils.isNotEmpty(accountDisplayNames) ? accountDisplayNames : new HashMap<>();
            fields = new ArrayList<>(displayNames.keySet());
            Collections.sort(fields);
            Map<String, String> changedContactDisplayNames = MapUtils.isNotEmpty(contactDisplayNames) ? contactDisplayNames : new HashMap<>();
            changedContactDisplayNames = changedContactDisplayNames.entrySet().stream().collect(Collectors.toMap(entry -> DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + entry.getKey(),
                    entry -> entry.getValue()));
            List<String> contactFields = new ArrayList<>(changedContactDisplayNames.keySet());
            Collections.sort(contactFields);
            fields.addAll(contactFields);
            displayNames.putAll(changedContactDisplayNames);
        }
        generateRecommendationCSVContext.setFields(fields);
        generateRecommendationCSVContext.setDisplayNames(displayNames);
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

    private boolean shouldIgnoreAccountsWithoutContacts(DeltaCampaignLaunchExportFilesGeneratorConfiguration config) {
        return config.getDestinationSysType() == CDLExternalSystemType.MAP
                || ChannelConfigUtil.isContactAudienceType(config.getDestinationSysName(), config.getChannelConfig());
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String addRenameCSVPath;
        String deleteRenameCSVPath;
        Date fileExportTime = new Date();
        if (createAddCsvDataFrame && createDeleteCsvDataFrame) {
            addRenameCSVPath = renameDataUnit(result.getTargets().get(0), ADD_FILE_PREFIX, fileExportTime);
            putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_FILES, Lists.newArrayList(addRenameCSVPath));
            deleteRenameCSVPath = renameDataUnit(result.getTargets().get(1), DELETE_FILE_PREFIX, fileExportTime);
            putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_FILES, Lists.newArrayList(deleteRenameCSVPath));
        } else if (createAddCsvDataFrame) {
            addRenameCSVPath = renameDataUnit(result.getTargets().get(0), ADD_FILE_PREFIX, fileExportTime);
            putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_FILES, Lists.newArrayList(addRenameCSVPath));
        } else {
            deleteRenameCSVPath = renameDataUnit(result.getTargets().get(0), DELETE_FILE_PREFIX, fileExportTime);
            putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_FILES, Lists.newArrayList(deleteRenameCSVPath));
        }
    }

    private String renameDataUnit(HdfsDataUnit hdfsDataUnit, String filePrefix, Date fileExportTime) {
        String fileFormat = "csv";
        String fileName = String.format("Recommendations_%s_%s.%s", filePrefix, DateTimeUtils.currentTimeAsString(fileExportTime), fileFormat);
        try {
            String outputDir = hdfsDataUnit.getPath();
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, outputDir, (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv"));
            String csvPath = files.get(0);
            String renamePath = outputDir + "/" + fileName;
            HdfsUtils.rename(yarnConfiguration, csvPath, renamePath);
            return renamePath;
        } catch (IOException e) {
            log.info("Can't rename CSV file to {}.", fileName);
            throw new RuntimeException("Unable to rename csv file on HDFS!");
        }
    }
}
