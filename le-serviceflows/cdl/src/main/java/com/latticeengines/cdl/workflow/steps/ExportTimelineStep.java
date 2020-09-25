package com.latticeengines.cdl.workflow.steps;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.TimelineExportRequest;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ExportTimelineSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ExportTimelineJobConfig;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.ExportTimelineJob;

@Component(ExportTimelineStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class ExportTimelineStep extends RunSparkJob<ExportTimelineSparkStepConfiguration, ExportTimelineJobConfig> {

    static final String BEAN_NAME = "exportTimelineStep";
    private static Logger log = LoggerFactory.getLogger(ExportTimelineStep.class);
    public static final String DATE_ONLY_FORMAT_STRING = "yyyy-MM-dd";
    public static final String EXPORT_TIMELINE_PREFIX = "exportTimeline_";

    @Override
    protected Class<? extends AbstractSparkJob<ExportTimelineJobConfig>> getJobClz() {
        return ExportTimelineJob.class;
    }

    @Override
    protected ExportTimelineJobConfig configureJob(ExportTimelineSparkStepConfiguration stepConfiguration) {
        Map<String, String> timelineTableNames = configuration.getTimelineTableNames();
        Table latticeAccountTable = configuration.getLatticeAccountTable();
        if (MapUtils.isEmpty(timelineTableNames) || latticeAccountTable == null) {
            log.info("timelineTable is empty or latticeAccountTable is null, skip this step.");
            return null;
        }
        TimelineExportRequest request = configuration.getRequest();
        List<DataUnit> inputs = new ArrayList<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_ONLY_FORMAT_STRING);
        dateFormat.setTimeZone(TimeZone.getTimeZone(request.getTimezone()));
        ExportTimelineJobConfig config = new ExportTimelineJobConfig();
        try {
            if (!StringUtils.isEmpty(request.getFromDate())) {
                config.fromDateTimestamp = dateFormat.parse(request.getFromDate()).getTime();
            }
            if (!StringUtils.isEmpty(request.getToDate())) {
                config.toDateTimestamp = dateFormat.parse(request.getToDate()).getTime();
            }
        } catch (ParseException e) {
            log.info("Can't parse fromDate {} in timelineExportRequest", request.getFromDate());
            return null;
        }
        if (!CollectionUtils.isEmpty(request.getEventTypes())) {
            config.eventTypes = request.getEventTypes();
        }
        config.timelineTableNames = timelineTableNames;
        toDataUnits(new ArrayList<>(timelineTableNames.values()), config.inputIdx, inputs);
        config.latticeAccountTableIdx = inputs.size();
        inputs.add(latticeAccountTable.toHdfsDataUnit("LatticeAccount"));
        HdfsDataUnit timelineUniverseAccountList = getObjectFromContext(TIMELINE_EXPORT_ACCOUNTLIST,
                HdfsDataUnit.class);
        if (timelineUniverseAccountList != null) {
            config.timelineUniverseAccountListIdx = inputs.size();
            inputs.add(timelineUniverseAccountList);

        }
        config.setInput(inputs);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputStr = result.getOutput();
        Map<?, ?> rawMap = JsonUtils.deserialize(outputStr, Map.class);
        Map<String, Integer> timelineOutputIdx = JsonUtils.convertMap(rawMap, String.class, Integer.class);
        Preconditions.checkArgument(MapUtils.isNotEmpty(timelineOutputIdx),
                "timeline output index map should not be empty here");
        timelineOutputIdx.forEach((timelineId, outputIdx) -> {

        });
    }

    private List<HdfsDataUnit> toDataUnits(List<String> tableNames, Map<String, Integer> inputIdx,
                                           List<DataUnit> inputs) {
        if (CollectionUtils.isEmpty(tableNames)) {
            return Collections.emptyList();
        }

        return tableNames.stream() //
                .map(name -> {
                    inputIdx.put(name, inputs.size());
                    return metadataProxy.getTable(configuration.getCustomer(), name);
                }) //
                .map(table -> {
                    HdfsDataUnit du = table.toHdfsDataUnit(null);
                    inputs.add(du);
                    return du;
                }) //
                .collect(Collectors.toList());
    }
}
