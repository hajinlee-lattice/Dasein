package com.latticeengines.datacloud.yarn.runtime;

import javax.annotation.Resource;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.aspect.MatchStepAspect;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("dataCloudProcessor")
public class DataCloudProcessor extends SingleContainerYarnProcessor<DataCloudJobConfiguration> {

    private static final Log log = LogFactory.getLog(DataCloudProcessor.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ApplicationContext appContext;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @Autowired
    private VersionManager versionManager;

    @Autowired
    private MatchActorSystem matchActorSystem;

    @Autowired
    private ProcessorContext initialProcessorContext;

    @Resource(name = "bulkMatchProcessorExecutor")
    private BulkMatchProcessorExecutor bulkMatchProcessorExecutor;
    @Resource(name = "bulkMatchProcessorAsyncExecutor")
    private BulkMatchProcessorExecutor bulkMatchProcessorAsyncExecutor;

    @Override
    public String process(DataCloudJobConfiguration jobConfiguration) throws Exception {
        try {
            appContext = softwareLibraryService.loadSoftwarePackages("datacloud", appContext, versionManager);
            LogManager.getLogger(MatchStepAspect.class).setLevel(Level.DEBUG);
            matchActorSystem.setBatchMode(true);

            initialProcessorContext.initialize(this, jobConfiguration);
            if (initialProcessorContext.isUseRemoteDnB()) {
                bulkMatchProcessorAsyncExecutor.execute(initialProcessorContext);
                bulkMatchProcessorAsyncExecutor.finalize(initialProcessorContext);

            } else {
                bulkMatchProcessorExecutor.execute(initialProcessorContext);
                bulkMatchProcessorExecutor.finalize(initialProcessorContext);
            }

        } catch (Exception e) {
            String rootOperationUid = jobConfiguration.getRootOperationUid();
            String blockOperationUid = jobConfiguration.getBlockOperationUid();
            String errFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid)
                    .toString();
            try {
                HdfsUtils.writeToFile(yarnConfiguration, errFile, ExceptionUtils.getFullStackTrace(e));
            } catch (Exception e1) {
                log.error("Failed to write error to err file.", e1);
            }
            throw (e);
        }

        return null;
    }

}
