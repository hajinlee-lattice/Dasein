package com.latticeengines.dataflowapi.yarn.runtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.dataflow.ExtractFilter;
import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.flink.FlinkConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public class DataFlowProcessor extends SingleContainerYarnProcessor<DataFlowConfiguration> {

    private static final Log log = LogFactory.getLog(DataFlowProcessor.class);

    @Autowired
    private ApplicationContext appContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private VersionManager versionManager;

    @Value("${dataflowapi.checkpoint:false}")
    private boolean checkpoint;

    @Value("${dataflowapi.engine}")
    private String engine;

    @Value("${dataflowapi.flink.mode}")
    private String flinkMode;

    @Value("${dataflowapi.flink.yarn.containers}")
    private Integer flinkYarnContainers;

    @Value("${dataflowapi.flink.yarn.slots}")
    private Integer flinkYarnSlots;

    @Value("${dataflowapi.flink.yarn.tm.mem.mb}")
    private Integer flinkYarnTmMem;

    @Value("${dataflowapi.flink.yarn.jm.mem.mb}")
    private Integer flinkYarnJmMem;

    public DataFlowProcessor() {
        super();
    }

    @Override
    public String process(DataFlowConfiguration dataFlowConfig) throws Exception {
        log.info("Running processor.");
        appContext = loadSoftwarePackages("dataflowapi", softwareLibraryService, appContext, versionManager);
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
                log.info(String.format("The first extract of table %s is located at %s", name, sourceTable
                        .getExtracts().get(0).getPath()));
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
            for (Table sourceTable : sourceTables.values()) {
                table.addDependency(DependableObject.fromDependable(sourceTable));
            }
            metadataProxy.updateTable(dataFlowConfig.getCustomerSpace().toString(), table.getName(), table);
        }
        return null;
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
        ctx.setProperty(DataFlowProperty.FLINKMODE, flinkMode);
        ctx.setProperty(DataFlowProperty.APPCTX, appContext);
        ctx.setProperty(DataFlowProperty.PARAMETERS, dataFlowConfig.getDataFlowParameters());
        Integer partitions = dataFlowConfig.getPartitions();
        if (partitions != null) {
            ctx.setProperty(DataFlowProperty.PARTITIONS, partitions);
        }
        if (dataFlowConfig.getJobProperties() != null) {
            ctx.setProperty(DataFlowProperty.JOBPROPERTIES, dataFlowConfig.getJobProperties());
        }

        if ("yarn".equals(flinkMode)) {
            org.apache.flink.configuration.Configuration flinkConf = new org.apache.flink.configuration.Configuration();
            flinkConf.setString(FlinkConstants.JM_HEAP_CONF, String.valueOf(flinkYarnJmMem));
            flinkConf.setString(FlinkConstants.TM_HEAP_CONF, String.valueOf(flinkYarnTmMem));
            flinkConf.setString(FlinkConstants.NUM_CONTAINERS, String.valueOf(flinkYarnContainers));
            flinkConf.setString(FlinkConstants.TM_SLOTS, String.valueOf(flinkYarnSlots));
            if (dataFlowConfig.getJobProperties() != null) {
                for (Map.Entry<Object, Object> entry : dataFlowConfig.getJobProperties().entrySet()) {
                    if (entry.getKey() instanceof String && entry.getValue() != null) {
                        flinkConf.setString((String) entry.getKey(), String.valueOf(entry.getValue()));
                    }
                }
            }
            ctx.setProperty(DataFlowProperty.FLINKCONF, flinkConf);
        }

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
