package com.latticeengines.cdl.workflow.steps;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.inject.Inject;

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
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.TimelineExportRequest;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ExportTimelineSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateTimelineExportArtifactsJobConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateTimelineExportArtifacts;

@Component(ExportTimelineStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class ExportTimelineStep extends RunSparkJob<ExportTimelineSparkStepConfiguration, GenerateTimelineExportArtifactsJobConfig> {

    static final String BEAN_NAME = "exportTimelineStep";
    private static Logger log = LoggerFactory.getLogger(ExportTimelineStep.class);
    public static final String DATE_ONLY_FORMAT_STRING = "yyyy-MM-dd";
    public static final String EXPORT_TIMELINE_SUFFIX = "exportTimeline";
    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected Class<? extends AbstractSparkJob<GenerateTimelineExportArtifactsJobConfig>> getJobClz() {
        return GenerateTimelineExportArtifacts.class;
    }

    @Override
    protected GenerateTimelineExportArtifactsJobConfig configureJob(ExportTimelineSparkStepConfiguration stepConfiguration) {
        Map<String, String> timelineTableNames = configuration.getTimelineTableNames();
        Table latticeAccountTable = dataCollectionProxy.getTable(configuration.getCustomer(), TableRoleInCollection.LatticeAccount,
                configuration.getVersion());
        if (MapUtils.isEmpty(timelineTableNames) || latticeAccountTable == null) {
            log.info("timelineTable is empty or latticeAccountTable is null, skip this step.");
            return null;
        }
        TimelineExportRequest request = configuration.getRequest();
        List<DataUnit> inputs = new ArrayList<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_ONLY_FORMAT_STRING);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(request.getTimezone()));
        GenerateTimelineExportArtifactsJobConfig config = new GenerateTimelineExportArtifactsJobConfig();
        try {
            if (StringUtils.isNotEmpty(request.getFromDate())) {
                calendar.setTime(dateFormat.parse(request.getFromDate()));
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                config.fromDateTimestamp = calendar.getTimeInMillis();
            }
            if (StringUtils.isNotEmpty(request.getToDate())) {
                calendar.setTime(dateFormat.parse(request.getToDate()));
                calendar.add(Calendar.DATE, 1);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                config.toDateTimestamp = calendar.getTimeInMillis();
            }
        } catch (ParseException e) {
            log.error("Can't parse fromDate {} in timelineExportRequest", request.getFromDate());
            return null;
        }
        if (CollectionUtils.isNotEmpty(request.getEventTypes())) {
            config.eventTypes = request.getEventTypes();
        }
        config.timelineTableNames = timelineTableNames;
        config.timeZone = request.getTimezone();
        config.rollupToDaily = request.isRollupToDaily();
        toDataUnits(new ArrayList<>(timelineTableNames.values()), config.inputIdx, inputs);
        config.latticeAccountTableIdx = inputs.size();
        inputs.add(latticeAccountTable.toHdfsDataUnit("LatticeAccount"));
        HdfsDataUnit timelineUniverseAccountList = getObjectFromContext(TIMELINE_EXPORT_ACCOUNTLIST,
                HdfsDataUnit.class);
        if (timelineUniverseAccountList != null) {
            config.accountListIdx = inputs.size();
            inputs.add(timelineUniverseAccountList);

        }
        config.setInput(inputs);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputStr = result.getOutput();
        Map<?, ?> rawMap = JsonUtils.deserialize(outputStr, Map.class);
        List<String> tablePaths = new ArrayList<>();
        Map<String, Integer> timelineOutputIdx = JsonUtils.convertMap(rawMap, String.class, Integer.class);
        Preconditions.checkArgument(MapUtils.isNotEmpty(timelineOutputIdx),
                "timeline output index map should not be empty here");
        timelineOutputIdx.forEach((timelineId, outputIdx) -> {
            String exportTableName = String.format("%s_%s", timelineId,
                    NamingUtils.timestamp(EXPORT_TIMELINE_SUFFIX));
            Table table = toTable(exportTableName, result.getTargets().get(outputIdx));
            log.info("Create timeline export table {} for timeline ID {}", exportTableName, timelineId);
            tablePaths.addAll(table.getExtracts().stream().map(Extract::getPath).collect(Collectors.toSet()));
        });
        log.info("tablePaths is {}", tablePaths);
        putObjectInContext(TIMELINE_EXPORT_TABLES, tablePaths);
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
