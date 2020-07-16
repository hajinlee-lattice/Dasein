package com.latticeengines.dcp.workflow.steps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.InputPresenceConfig;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.dcp.InputPresenceJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AnalyzeInput extends RunSparkJob<ImportSourceStepConfiguration, InputPresenceConfig> {

    static final Map<MatchKey, String> MATCH_KEYS_TO_INTERNAL_NAMES = new HashMap<>();
    static {
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Name, InterfaceName.CompanyName.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Address, InterfaceName.Address_Street_1.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Address2, InterfaceName.Address_Street_2.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.City, InterfaceName.City.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.State, InterfaceName.State.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Country, InterfaceName.Country.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Zipcode, InterfaceName.PostalCode.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.DUNS, InterfaceName.DUNS.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Domain, InterfaceName.Website.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Email, InterfaceName.Email.name());
    }

    private static final Logger log = LoggerFactory.getLogger(AnalyzeInput.class);

    @Inject
    private DataReportProxy dataReportProxy;

    @Inject
    private UploadProxy uploadProxy;

    @Override
    protected Class<? extends AbstractSparkJob<InputPresenceConfig>> getJobClz() {
        return InputPresenceJob.class;
    }

    @Override
    protected InputPresenceConfig configureJob(ImportSourceStepConfiguration stepConfiguration) {
        String inputPath = getStringValueFromContext(IMPORT_DATA_LOCATION);
        if (StringUtils.isBlank(inputPath)) {
            throw new IllegalStateException("Cannot find import data location from context");
        }
        int index = inputPath.indexOf("/Pods/");
        if (index > 0) {
            log.info("initial path is {}" , inputPath);
            inputPath = inputPath.substring(index);
        }
        HdfsDataUnit unit = HdfsDataUnit.fromPath(inputPath);
        InputPresenceConfig config = new InputPresenceConfig();
        config.setInput(Collections.singletonList(unit));
        config.setInputNames(MATCH_KEYS_TO_INTERNAL_NAMES.values()
                .stream().filter(Objects::nonNull).collect(Collectors.toSet()));
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        Map<String, Long> map = JsonUtils.convertMap(JsonUtils.deserialize(result.getOutput(), Map.class),
                String.class, Long.class);

        UploadStats stats = getObjectFromContext(UPLOAD_STATS, UploadStats.class);
        UploadStats.ImportStats importStats = stats.getImportStats();
        long ingested = importStats.getSuccessfullyIngested();
        DataReport.InputPresenceReport inputPresenceReport = new DataReport.InputPresenceReport();
        map.forEach((name, populated) -> inputPresenceReport.addPresence(name, populated, ingested));

        dataReportProxy.updateDataReport(configuration.getCustomerSpace().toString(), DataReportRecord.Level.Upload,
                configuration.getUploadId(), inputPresenceReport);

        String uploadId = configuration.getUploadId();
        uploadProxy.updateUploadStatus(customerSpace.toString(), uploadId, Upload.Status.IMPORT_FINISHED, null);
        uploadProxy.updateProgressPercentage(customerSpace.toString(), uploadId, "33");
    }
}
