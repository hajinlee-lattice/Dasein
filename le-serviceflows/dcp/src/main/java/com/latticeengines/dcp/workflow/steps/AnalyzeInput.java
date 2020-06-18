package com.latticeengines.dcp.workflow.steps;

import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.InputPresenceConfig;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.dcp.InputPresenceJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AnalyzeInput extends RunSparkJob<ImportSourceStepConfiguration, InputPresenceConfig> {

    static final Map<MatchKey, String> MATCH_KEYS_TO_INTERNAL_NAMES = new HashMap<>();
    static {
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Name, InterfaceName.CompanyName.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.City, InterfaceName.City.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.State, InterfaceName.State.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Country, InterfaceName.Country.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.Zipcode, InterfaceName.PostalCode.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.DUNS, InterfaceName.DUNS.name());
        MATCH_KEYS_TO_INTERNAL_NAMES.put(MatchKey.ExternalId, InterfaceName.Id.name());
    }

    private static final Logger log = LoggerFactory.getLogger(AnalyzeInput.class);

    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;


    @Inject
    private DataReportProxy dataReportProxy;

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

        ImportWorkflowSpec spec =
                importWorkflowSpecProxy.getImportWorkflowSpec(configuration.getCustomerSpace().toString(),
                S3ImportSystem.SystemType.DCP.name(), EntityType.Accounts.getDisplayName());
        Map<String, String> fieldNameToDisplayName =
                spec.getFieldDefinitionsRecordsMap().values()
                        .stream()
                        .reduce(new ArrayList<>(), (x, y) -> { x.addAll(y); return x;})
                        .stream()
                        .filter(e -> StringUtils.isNotBlank(e.getFieldName())
                                && StringUtils.isNotBlank(e.getScreenName()))
                        .collect(Collectors.toMap(FieldDefinition::getFieldName, FieldDefinition::getScreenName));

        UploadStats stats = getObjectFromContext(UPLOAD_STATS, UploadStats.class);
        UploadStats.ImportStats importStats = stats.getImportStats();
        long ingested = importStats.getSuccessfullyIngested();
        DataReport.InputPresenceReport inputPresenceReport = new DataReport.InputPresenceReport();
        MATCH_KEYS_TO_INTERNAL_NAMES.forEach((key, name) -> {
            long populated = map.getOrDefault(name, 0L);
            inputPresenceReport.addPresence(fieldNameToDisplayName.getOrDefault(name, "empty"), populated, ingested);
        });

        dataReportProxy.updateDataReport(configuration.getCustomerSpace().toString(), DataReportRecord.Level.Upload,
                configuration.getUploadId(), inputPresenceReport);
    }
}
