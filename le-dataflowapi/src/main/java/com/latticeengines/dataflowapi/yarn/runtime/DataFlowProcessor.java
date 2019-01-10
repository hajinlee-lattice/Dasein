package com.latticeengines.dataflowapi.yarn.runtime;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowJob;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.dataflow.ExtractFilter;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

public class DataFlowProcessor extends SingleContainerYarnProcessor<DataFlowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DataFlowProcessor.class);

    @Autowired
    private ApplicationContext appContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${dataflowapi.checkpoint:false}")
    private boolean checkpoint;

    @Value("${dataflowapi.engine}")
    private String engine;

    public DataFlowProcessor() {
        super();
    }

    @Override
    public String process(DataFlowConfiguration dataFlowConfig) throws Exception {
        log.info("Running processor.");
        String swlib = dataFlowConfig.getSwlib();
        if (StringUtils.isNotBlank(swlib)) {
            log.info("Enriching application context with sw package " + swlib);
            appContext = softwareLibraryService.loadSoftwarePackages("dataflowapi", Collections.singleton(swlib),
                    appContext);
        } else {
            log.info("Enriching application context with all sw packages available.");
            appContext = softwareLibraryService.loadSoftwarePackages("dataflowapi", appContext);
        }
        Map<String, Table> sourceTables = new HashMap<>();

        List<DataFlowSource> dataFlowSources = dataFlowConfig.getDataSources();

        for (DataFlowSource dataFlowSource : dataFlowSources) {
            String name = dataFlowSource.getName();

            log.info(String.format("Retrieving source table %s for customer space %s", name,
                    dataFlowConfig.getCustomerSpace()));
            Table sourceTable = metadataProxy.getTable(dataFlowConfig.getCustomerSpace().toString(), name);
            if (sourceTable == null) {
                log.error("Source table " + name + " retrieved from the metadata service is null.");
                continue;
            }
            if (sourceTable.getExtracts().size() > 0) {
                log.info(String.format("The first extract of table %s is located at %s", name,
                        sourceTable.getExtracts().get(0).getPath()));
            }
            sourceTables.put(name, sourceTable);
        }

        DataFlowContext ctx = getDataFlowContext(dataFlowConfig, sourceTables);

        String property = String.format("dataflowapi.flow.%s.debug", dataFlowConfig.getDataFlowBeanName());
        String debugStr = PropertyUtils.getProperty(property);
        log.info(String.format("%s: %s", property, debugStr));
        boolean debug = Boolean.parseBoolean(debugStr);
        ctx.setProperty(DataFlowProperty.DEBUG, debug);

        log.info(String.format("Running data transform with bean %s", dataFlowConfig.getDataFlowBeanName()));
        Table table = dataTransformationService.executeNamedTransformation(ctx, dataFlowConfig.getDataFlowBeanName());
        log.info(String.format("Setting metadata for table %s", table.getName()));
        if (!dataFlowConfig.shouldSkipRegisteringTable()) {
            metadataProxy.updateTable(dataFlowConfig.getCustomerSpace().toString(), table.getName(), table);
        }
        updateJob(table, sourceTables);
        return null;
    }

    private void updateJob(Table table, Map<String, Table> sourceTables) {
        try {
            DataFlowJob job = (DataFlowJob) jobEntityMgr.findByObjectId(appId.toString());
            int count = 0;
            while (job == null && count++ < 10) {
                log.info("Retrieving job");
                job = (DataFlowJob) jobEntityMgr.findByObjectId(appId.toString());
                Thread.sleep(1000);
            }
            if (job == null) {
                log.error(String.format("Could not locate job with appId %s", appId));
                return;
            }
            if (table != null) {
                job.setTargetTableName(table.getName());
            }
            job.setSourceTableNames(sourceTables.values().stream().map(t -> t.getName()).collect(Collectors.toList()));
            jobEntityMgr.update(job);
        } catch (Exception e) {
            log.error("Failed to update job", e);
        }
    }

    private DataFlowContext getDataFlowContext(DataFlowConfiguration dataFlowConfig, Map<String, Table> sourceTables) {
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, dataFlowConfig.getTargetTableName());
        ctx.setProperty(DataFlowProperty.CUSTOMER, dataFlowConfig.getCustomerSpace().toString());

        ctx.setProperty(DataFlowProperty.SOURCETABLES, sourceTables);
        String targetPath = dataFlowConfig.getTargetPath();
        String namespace = dataFlowConfig.getNamespace();
        if (StringUtils.isBlank(targetPath)) {
            Path baseTargetPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                    dataFlowConfig.getCustomerSpace(), namespace);
            targetPath = baseTargetPath.append(dataFlowConfig.getTargetTableName()).toString();
        } else if (!StringUtils.isBlank(namespace)) {
            String namespacePath = StringUtils.join(namespace.split("\\."), "/");
            targetPath += "/" + namespacePath;
        }

        log.info(String.format("Target path is %s", targetPath));
        ctx.setProperty(DataFlowProperty.EXTRACTFILTERS, getExtractFilters(dataFlowConfig));
        ctx.setProperty(DataFlowProperty.TARGETPATH, targetPath);
        if (StringUtils.isNotEmpty(dataFlowConfig.getQueue())) {
            ctx.setProperty(DataFlowProperty.QUEUE, dataFlowConfig.getQueue());
        } else {
            ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        }
        ctx.setProperty(DataFlowProperty.FLOWNAME, dataFlowConfig.getDataFlowBeanName());
        ctx.setProperty(DataFlowProperty.CHECKPOINT, checkpoint);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, yarnConfiguration);
        if (StringUtils.isNotEmpty(dataFlowConfig.getEngine())) {
            ctx.setProperty(DataFlowProperty.ENGINE, dataFlowConfig.getEngine());
        } else {
            ctx.setProperty(DataFlowProperty.ENGINE, engine);
        }
        ctx.setProperty(DataFlowProperty.APPCTX, appContext);
        ctx.setProperty(DataFlowProperty.ENFORCEGLOBALORDERING, false);
        ctx.setProperty(DataFlowProperty.PARAMETERS, dataFlowConfig.getDataFlowParameters());
        Integer partitions = dataFlowConfig.getPartitions();
        if (partitions != null) {
            ctx.setProperty(DataFlowProperty.PARTITIONS, partitions);
        }
        if (dataFlowConfig.getJobProperties() != null) {
            ctx.setProperty(DataFlowProperty.JOBPROPERTIES, dataFlowConfig.getJobProperties());
        }
        if (dataFlowConfig.isApplyTableProperties())
            ctx.setProperty(DataFlowProperty.APPLYTABLEPROPERTIES, Boolean.TRUE);
        return ctx;
    }

    private Map<String, List<ExtractFilter>> getExtractFilters(DataFlowConfiguration dataFlowConfiguration) {
        Map<String, List<ExtractFilter>> extractFilters = new HashMap<>();
        for (DataFlowSource source : dataFlowConfiguration.getDataSources()) {
            if (source.getExtractFilters() != null && source.getExtractFilters().size() > 0) {
                extractFilters.put(source.getName(), source.getExtractFilters());
            }
        }

        return extractFilters;
    }
}
