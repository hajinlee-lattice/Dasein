package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.MathUtils;
import com.latticeengines.domain.exposed.pls.ReportPurpose;
import com.latticeengines.prospectdiscovery.dataflow.CreateImportSummary;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("registerImportSummaryReport")
public class RegisterImportSummaryReport extends BaseWorkflowStep<TargetMarketStepConfiguration> {

    private RegisterImportSummary registerImportSummary = new RegisterImportSummary();

    @Override
    public void execute() {
        registerImportSummary.setConfiguration(configuration);
        registerImportSummary.setRestTemplate(restTemplate);
        registerImportSummary.execute("CreateImportSummary", ReportPurpose.IMPORT_SUMMARY);
    }
    
    public RegisterReport getReportRegistrator() {
        return registerImportSummary;
    }

    private class RegisterImportSummary extends RegisterReport {

        public ObjectNode buildJson(GenericRecord stats) {
            ObjectNode json = new ObjectMapper().createObjectNode();

            // accounts
            ObjectNode accounts = json.putObject("accounts");
            accounts.put("total", (Long) stats.get(CreateImportSummary.TOTAL_ACCOUNTS));
            accounts.put("with_contacts",
                    (Long) stats.get(CreateImportSummary.TOTAL_ACCOUNTS_WITH_CONTACTS));
            accounts.put("with_opportunities",
                    (Long) stats.get(CreateImportSummary.TOTAL_ACCOUNTS_WITH_OPPORTUNITIES));
            accounts.put("unique", (Long) stats.get(CreateImportSummary.TOTAL_UNIQUE_ACCOUNTS));

            long eventTableMatched = ((Long) stats.get(CreateImportSummary.TOTAL_MATCHED_ACCOUNTS));
            long eventTableTotal = ((Long) stats.get(CreateImportSummary.TOTAL_UNIQUE_ACCOUNTS));

            if (eventTableMatched == 0 || eventTableTotal == 0) {
                accounts.put("match_rate", 0.0);
            } else {
                accounts.put("match_rate", (double) eventTableMatched / (double) eventTableTotal);
            }

            // contacts
            ObjectNode contacts = json.putObject("contacts");
            contacts.put("total", (Long) stats.get(CreateImportSummary.TOTAL_CONTACTS));

            // leads
            ObjectNode leads = json.putObject("leads");
            leads.put("total", (Long) stats.get(CreateImportSummary.TOTAL_LEADS));

            // opportunities
            leads.put("total", (Long) stats.get(CreateImportSummary.TOTAL_OPPORTUNITIES));
            leads.put("closed", (Long) stats.get(CreateImportSummary.TOTAL_CLOSED_OPPORTUNITIES));
            leads.put("closed_won",
                    (Long) stats.get(CreateImportSummary.TOTAL_CLOSED_WON_OPPORTUNITIES));

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
    }

}
