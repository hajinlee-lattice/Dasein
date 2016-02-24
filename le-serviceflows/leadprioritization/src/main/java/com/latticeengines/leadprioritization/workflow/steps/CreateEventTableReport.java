package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;
import com.latticeengines.serviceflows.workflow.report.BaseReportStepConfiguration;

@Component("createEventTableReport")
public class CreateEventTableReport extends BaseReportStep<BaseReportStepConfiguration> {
    private static final Logger log = Logger.getLogger(CreateEventTableReport.class);

    @Autowired
    private JobProxy jobProxy;

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.EVENT_TABLE_IMPORT_SUMMARY;
    }

    @Override
    protected ObjectNode getJson() {
        try {
            String applicationId = getObjectFromContext(IMPORT_DATA_APPLICATION_ID, String.class);
            if (applicationId == null) {
                throw new RuntimeException(
                        "Could not generate report.  Application ID for import has not been saved in context");
            }
            log.info(String.format("Generating report for application id %s", applicationId));

            Counters counters = jobProxy.getMRJobCounters(applicationId);

            if (counters == null) {
                throw new RuntimeException(String.format(
                        "Could not generate report.  Counters for import application id %s are null", applicationId));
            }

            CounterGroup group = counters.getCounterGroup(RecordImportCounter.class.getName());

            ObjectNode json = new ObjectMapper().createObjectNode();
            json.put("imported_records", group.getCounter(RecordImportCounter.IMPORTED_RECORDS.toString()).getValue());
            json.put("ignored_records", group.getCounter(RecordImportCounter.IGNORED_RECORDS.toString()).getValue());
            json.put("required_field_missing", group.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING.toString())
                    .getValue());
            json.put("field_malformed", group.getCounter(RecordImportCounter.FIELD_MALFORMED.toString()).getValue());
            json.put("row_error", group.getCounter(RecordImportCounter.ROW_ERROR.toString()).getValue());
            return json;
        } catch (Exception e) {
            log.error("Could not generate report", e);
            return null;
        }
    }
}
