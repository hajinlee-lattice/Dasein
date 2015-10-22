package com.latticeengines.dataflowapi.yarn.runtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.dataflowapi.util.MetadataProxy;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
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
    private MetadataProxy proxy;

    public DataFlowProcessor() {
        super();
    }

    @Override
    public String process(DataFlowConfiguration dataFlowConfig) throws Exception {
        log.info("Running processor.");
        appContext = loadSoftwarePackages("dataflowapi", softwareLibraryService, appContext);
        Map<String, String> sources = new HashMap<>();
        Map<String, Table> sourceTables = new HashMap<>();

        List<DataFlowSource> dataFlowSources = dataFlowConfig.getDataSources();

        boolean usesTables = false;
        boolean usesPaths = false;
        for (DataFlowSource dataFlowSource : dataFlowSources) {
            String name = dataFlowSource.getName();

            if (dataFlowSource.getRawDataPath() != null) {
                sources.put(name, dataFlowSource.getRawDataPath());
                usesPaths = true;
            } else {
                sourceTables.put(name, proxy.getMetadata(dataFlowConfig.getCustomerSpace(), name));
                usesTables = true;
            }
        }

        if (usesPaths && usesTables) {
            throw new LedpException(LedpCode.LEDP_27005);
        }

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("TARGETTABLENAME", dataFlowConfig.getName());
        ctx.setProperty("CUSTOMER", dataFlowConfig.getCustomerSpace().toString());

        if (usesTables) {
            ctx.setProperty("SOURCETABLES", sourceTables);
        } else {
            ctx.setProperty("SOURCES", sources);
        }
        Path baseTargetPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                dataFlowConfig.getCustomerSpace());
        String targetPath = baseTargetPath.append(dataFlowConfig.getTargetPath()).toString();
        ctx.setProperty("TARGETPATH", targetPath);
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("FLOWNAME", dataFlowConfig.getDataFlowBeanName());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("ENGINE", "MR");
        ctx.setProperty("APPCTX", appContext);
        Table table = dataTransformationService.executeNamedTransformation(ctx, dataFlowConfig.getDataFlowBeanName());
        proxy.setMetadata(dataFlowConfig.getCustomerSpace(), table);
        return null;
    }
}
