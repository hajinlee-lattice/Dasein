package com.latticeengines.dcp.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.RollupDataReportStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.RollupDataReportConfig;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.dcp.RollupDataReportJob;

import jdk.internal.util.xml.impl.Input;
import parquet.Preconditions;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RollupDataReport extends RunSparkJob<RollupDataReportStepConfiguration, RollupDataReportConfig> {

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private DataReportProxy dataReportProxy;

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private SourceProxy sourceProxy;

    @Override
    protected Class<? extends AbstractSparkJob<RollupDataReportConfig>> getJobClz() {
        return RollupDataReportJob.class;
    }

    @Override
    protected RollupDataReportConfig configureJob(RollupDataReportStepConfiguration stepConfiguration) {
        String root = stepConfiguration.getRoot();
        DataReportRecord.Level level = stepConfiguration.getLevel();
        DataReportMode mode = stepConfiguration.getMode();

        DataReportRecord.Level rollupLevel = DataReportRecord.Level.Source;
        Map<String, Set<String>> parentIDToChildren = new HashMap<>();
        Map<String, Table> nameToTable = new HashMap<>();
        Map<String, Set<Integer>> idToInputIndex = new HashMap<>();
        List<Input> inputs = new ArrayList<>();
        int index = 0;
        switch (level) {
            case Tenant:
                List<ProjectSummary> projects = projectProxy.getAllDCPProject(customerSpace.toString(), true);
                parentIDToChildren.put(root, projects.stream().map(ProjectSummary::getProjectId).collect(Collectors.toSet()));
                projects.forEach(projectSummary -> {
                    parentIDToChildren.put(projectSummary.getProjectId(),
                            projectSummary.getSources().stream().map(Source::getSourceId).collect(Collectors.toSet()));
                    List<Source> sources = projectSummary.getSources();
                    sources.forEach(source -> {
                        String sourceId = source.getSourceId();
                        List<Table> tables = uploadProxy.getMatchResultsBySourceId(customerSpace.toString(), sourceId);
                        parentIDToChildren.put(source.getSourceId(), tables.stream().map(Table::getName).collect(Collectors.toSet()));
                        tables.forEach(table -> nameToTable.put(table.getName(), table));
                    });
                });
                break;
            case Project:
                ProjectDetails project = projectProxy.getDCPProjectByProjectId(customerSpace.toString(),
                        root, Boolean.TRUE);
                List<Source> sources = project.getSources();
                sources.forEach(source -> {
                    String sourceId = source.getSourceId();
                    List<Table> tables = uploadProxy.getMatchResultsBySourceId(customerSpace.toString(), sourceId);
                    parentIDToChildren.put(source.getSourceId(), tables.stream().map(Table::getName).collect(Collectors.toSet()));
                    tables.forEach(table -> nameToTable.put(table.getName(), table));
                });
                break;
            case Source:
                List<Table> tables = uploadProxy.getMatchResultsBySourceId(customerSpace.toString(), root);
                parentIDToChildren.put(root, tables.stream().map(Table::getName).collect(Collectors.toSet()));
                tables.forEach(table -> nameToTable.put(table.getName(), table));
                break;
            default:
                break;
        }

        return null;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {

    }
}
