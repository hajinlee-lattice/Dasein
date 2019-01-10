package com.latticeengines.cdl.workflow.steps.play;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("playLaunchExportFileGeneratorStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PlayLaunchExportFileGeneratorStep extends BaseWorkflowStep<PlayLaunchExportFilesGeneratorConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchExportFileGeneratorStep.class);

    static final Function<GenericRecord, GenericRecord> RecommendationJsonFormatter = new Function<GenericRecord, GenericRecord>() {
        @Override
        public GenericRecord apply(GenericRecord rec) {
            Object obj = rec.get("CONTACTS");
            if (obj != null && StringUtils.isNotBlank(obj.toString())) {
                obj = JsonUtils.deserialize(obj.toString(), new TypeReference<List<Map<String, String>>>() {
                });
                rec.put("CONTACTS", obj);
            }
            return rec;
        }
    };

    @Override
    public void execute() {
        PlayLaunchExportFilesGeneratorConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playLaunchId = config.getPlayLaunchId();

        String recAvroHdfsFilePath = getStringValueFromContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH);

        File localJsonFile = new File(String.format("pl_rec_%s_%s_%s_%s.json", customerSpace.getTenantId(), playLaunchId,
                config.getDestinationSysType(), System.currentTimeMillis()));
        log.info("Generating JSON File: %s", localJsonFile);
        try {
            AvroUtils.convertAvroToJSON(yarnConfiguration, new Path(recAvroHdfsFilePath), localJsonFile, RecommendationJsonFormatter);
            //TODO:Jaya: Replace marketo with DestinationSystemName 
            String namespace = String.format("%s.%s.%s.%s.%s.%s", config.getDestinationSysType(), "MARKETO", config.getDestinationOrgId(),
                    config.getPlayName(), config.getPlayLaunchId(), DateTimeUtils.currentTimeAsString());
            String path = PathBuilder.buildDataFileExportPath(CamilleEnvironment.getPodId(), customerSpace, namespace).toString();
            path = path.endsWith("/") ? path : path + "/";
            path += "Recommendations.json";

            String recFilePathForMarketo = path += "Recommendations.json";

            try {
                HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localJsonFile.getAbsolutePath(), recFilePathForMarketo);
            } finally {
                FileUtils.deleteQuietly(localJsonFile);
            }
            log.info("Uploaded recommendation to HDFS File: %s", recFilePathForMarketo);

            List<String> exportFiles = new ArrayList<>();
            exportFiles.add(recFilePathForMarketo);
            putObjectInContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_EXPORT_FILES, exportFiles);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18213, e);
        }
    }

}
