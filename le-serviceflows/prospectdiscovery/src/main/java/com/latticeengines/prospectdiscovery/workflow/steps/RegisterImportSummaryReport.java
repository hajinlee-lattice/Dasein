package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.MathUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.domain.exposed.pls.ReportPurpose;
import com.latticeengines.prospectdiscovery.dataflow.CreateImportSummary;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("registerImportSummaryReport")
public class RegisterImportSummaryReport extends BaseWorkflowStep<TargetMarketStepConfiguration> {
    private Configuration yarnConfiguration = new Configuration();

    @Override
    public void execute() {
        GenericRecord stats = retrieveStats();
        ObjectNode json = buildJson(stats);
        Report report = createReport(json.toString());

        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(configuration.getMicroServiceHostPort());
        proxy.registerReport(configuration.getTargetMarket().getName(), report, //
                configuration.getCustomerSpace().toString());
    }

    private GenericRecord retrieveStats() {
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", configuration.getMicroServiceHostPort(),
                configuration.getCustomerSpace(), "CreateImportSummary");
        Table table = restTemplate.getForObject(url, Table.class);

        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            paths.add(extract.getPath());
        }

        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, paths);
        if (records.size() == 0) {
            throw new RuntimeException("Failed to calculate report summary - zero records in avro file.");
        }

        return records.get(0);
    }

    private ObjectNode buildJson(GenericRecord stats) {
        ObjectNode json = new ObjectMapper().createObjectNode();

        // accounts
        ObjectNode accounts = json.putObject("accounts");
        accounts.put("total", (Long) stats.get(CreateImportSummary.TOTAL_ACCOUNTS));
        accounts.put("with_contacts", (Long) stats.get(CreateImportSummary.TOTAL_ACCOUNTS_WITH_CONTACTS));
        accounts.put("with_opportunities", (Long) stats.get(CreateImportSummary.TOTAL_ACCOUNTS_WITH_OPPORTUNITIES));
        accounts.put("unique", (Long) stats.get(CreateImportSummary.TOTAL_UNIQUE_ACCOUNTS));

        double matchRate = ((Double) stats.get(CreateImportSummary.TOTAL_UNIQUE_ACCOUNTS))
                / ((Double) stats.get(CreateImportSummary.TOTAL_MATCHED_ACCOUNTS));
        accounts.put("match_rate", matchRate);

        // contacts
        ObjectNode contacts = json.putObject("contacts");
        contacts.put("total", (Long) stats.get(CreateImportSummary.TOTAL_CONTACTS));

        // leads
        ObjectNode leads = json.putObject("leads");
        leads.put("total", (Long) stats.get(CreateImportSummary.TOTAL_LEADS));

        // opportunities
        leads.put("total", (Long) stats.get(CreateImportSummary.TOTAL_OPPORTUNITIES));
        leads.put("closed", (Long) stats.get(CreateImportSummary.TOTAL_CLOSED_OPPORTUNITIES));
        leads.put("closed_won", (Long) stats.get(CreateImportSummary.TOTAL_CLOSED_WON_OPPORTUNITIES));

        // date range
        List<Long> mins = new ArrayList<>();
        List<Long> maxes = new ArrayList<>();

        mins.add((Long) stats.get(CreateImportSummary.ACCOUNT_DATE_RANGE_BEGIN));
        mins.add((Long) stats.get(CreateImportSummary.CONTACT_DATE_RANGE_BEGIN));
        mins.add((Long) stats.get(CreateImportSummary.LEAD_DATE_RANGE_BEGIN));
        mins.add((Long) stats.get(CreateImportSummary.OPPORTUNITY_DATE_RANGE_BEGIN));

        maxes.add((Long) stats.get(CreateImportSummary.ACCOUNT_DATE_RANGE_END));
        maxes.add((Long) stats.get(CreateImportSummary.CONTACT_DATE_RANGE_END));
        maxes.add((Long) stats.get(CreateImportSummary.LEAD_DATE_RANGE_END));
        maxes.add((Long) stats.get(CreateImportSummary.OPPORTUNITY_DATE_RANGE_END));

        Long min = MathUtils.min(mins);
        Long max = MathUtils.max(maxes);

        ObjectNode dateRange = json.putObject("date_range");
        dateRange.put("begin", min);
        dateRange.put("end", max);
        return json;
    }

    private Report createReport(String json) {
        Report report = new Report();
        KeyValue kv = new KeyValue();
        kv.setPayload(json);
        report.setJson(kv);
        report.setPurpose(ReportPurpose.IMPORT_SUMMARY);
        return report;
    }
}
