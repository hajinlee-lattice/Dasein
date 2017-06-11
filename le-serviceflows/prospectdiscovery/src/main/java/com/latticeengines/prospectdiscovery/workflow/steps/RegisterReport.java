package com.latticeengines.prospectdiscovery.workflow.steps;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.TargetMarketStepConfiguration;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

public abstract class RegisterReport {
    private static final Logger log = Logger.getLogger(RegisterReport.class);

    private Configuration yarnConfiguration = new Configuration();
    private TargetMarketStepConfiguration configuration;
    private RestTemplate restTemplate;

    public abstract ObjectNode buildJson(List<GenericRecord> records, Object[] params);

    public void setConfiguration(TargetMarketStepConfiguration configuration) {
        this.configuration = configuration;
    }

    public void setRestTemplate(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public void execute(String reportName, ReportPurpose purpose, Object[] params) {
        List<GenericRecord> records = retrieveStats(reportName);
        ObjectNode json = buildJson(records, params);
        Report report = createReport(json.toString(), purpose);

        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(
                configuration.getInternalResourceHostPort());
        proxy.registerReport(configuration.getTargetMarket().getName(), report, //
                configuration.getCustomerSpace().toString());
    }

    protected List<GenericRecord> retrieveStats(String reportName) {
        log.info(String.format("fs.defaultFS: %s", yarnConfiguration.get("fs.defaultFS")));
        log.info(String.format("HADOOP_CONF_DIR: %s", System.getenv("HADOOP_CONF_DIR")));
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", configuration.getMicroServiceHostPort(),
                configuration.getCustomerSpace(), reportName);
        Table table = restTemplate.getForObject(url, Table.class);

        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            String path = extract.getPath();
            if (!path.endsWith("avro")) {
                paths.add(path + "/*.avro");
            }

        }

        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, paths);
        if (records.size() == 0) {
            throw new RuntimeException("Failed to calculate report summary - zero records in avro file.");
        }

        return records;
    }

    protected Report createReport(String json, ReportPurpose purpose) {
        Report report = new Report();
        KeyValue kv = new KeyValue();
        kv.setPayload(json);
        report.setJson(kv);
        report.setPurpose(purpose);
        return report;
    }

}
