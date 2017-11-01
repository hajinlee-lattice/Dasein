package com.latticeengines.pls.service.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.ModelCleanUpService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("modelCleanUpService")
public class ModelCleanUpServiceImpl implements ModelCleanUpService {
    private static Logger log = LoggerFactory.getLogger(ModelCleanUpServiceImpl.class);

    private static final String MODEL_SUMMARY_SUPPORTING_FILES_PATH = "/user/s-analytics/customers/%s/models/%s";
    private static final String MATCHED_AND_SCORED_TRAINING_CSV_FILES_PATH = "/user/s-analytics/customers/%s/data/%s";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public Boolean cleanUpModel(String modelId) {
        log.info(String.format("Clean up model, model id: %s", modelId));
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);

        if(modelSummary != null) {
            Tenant tenant = MultiTenantContext.getTenant();
            String customerSpace = tenant.getId();
            log.info(String.format("Clean up model, customer space: %s", customerSpace));

            log.info(String.format("Clean up model, event table name: %s", modelSummary.getEventTableName()));
            String eventTableExtractPath = getExtractPathByTableName(customerSpace, modelSummary.getEventTableName());
            removeDir("event table avro file", eventTableExtractPath);

            log.info(String.format("Clean up model, training table name: %s", modelSummary.getTrainingTableName()));
            String trainingTableExtractPath = getExtractPathByTableName(customerSpace, modelSummary.getTrainingTableName());
            removeDir("training data avro file", trainingTableExtractPath.substring(0, trainingTableExtractPath
                    .lastIndexOf("/")));

            SourceFile sourceFile = sourceFileService.findByTableName(modelSummary.getTrainingTableName());
            if(sourceFile != null) {
                removeDir("training table csv file", sourceFile.getPath());
            }

            removeDir("model summary supporting files", String.format(MODEL_SUMMARY_SUPPORTING_FILES_PATH,
                    customerSpace, modelSummary.getEventTableName()));

            removeDir("matched and scored training csv files", String.format(MATCHED_AND_SCORED_TRAINING_CSV_FILES_PATH,
                    customerSpace, modelSummary.getEventTableName()));

            removeDir("scored training avro file", getScordTrainingAvroFilePath(modelId, tenant));

            modelSummaryService.deleteModelSummaryByModelId(modelId);
        }
        else {
            log.warn(String.format("ModelSummary is not found by modelId: ", modelId));
        }

        return Boolean.TRUE;
    }

    private String getExtractPathByTableName(String customerSpace, String tableName) {
        String path = "";
        Table table = metadataProxy.getTable(customerSpace, tableName);
        if(table != null) {
            Extract extract = table.getExtracts().get(0);
            if(extract != null) {
                path = extract.getPath();
            }
        }

        return path;
    }

    private String getScordTrainingAvroFilePath(String modelId, Tenant tenant) {
        String path = "";
        try {
            HdfsUtils.HdfsFilenameFilter filter = new HdfsUtils.HdfsFilenameFilter() {
                @Override
                public boolean accept(String path) {
                    return path.contains(modelId.replace('-','_'));
                }
            };
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, PathBuilder.buildDataTablePath
                            (CamilleEnvironment.getPodId().toString(), CustomerSpace.parse(tenant.getName())).toString(),
                    filter);
            if (files.size() != 1) {
                throw new FileNotFoundException("Scored training avro file path is not found.");
            }

            path = files.get(0).replaceFirst(yarnConfiguration.get("fs.defaultFS"), "");
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        return path;
    }

    private void removeDir(String pathInfo, String path) {
        try {
            if(path != "") {
                log.info(String.format("Clean up model, Remove %s. Path: %s", pathInfo, path));
                HdfsUtils.rmdir(yarnConfiguration, path);
            }
        } catch (IOException e) {
            log.error(String.format("Clean up model, Remove %s error. Path: %s. Details: $s", pathInfo, path, e.getMessage()));
        }
    }
}
