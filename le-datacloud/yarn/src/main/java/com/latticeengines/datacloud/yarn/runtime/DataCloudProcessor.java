package com.latticeengines.datacloud.yarn.runtime;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.aspect.MatchStepAspect;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

@Component("dataCloudProcessor")
public class DataCloudProcessor extends SingleContainerYarnProcessor<DataCloudJobConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(DataCloudProcessor.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private MatchActorSystem matchActorSystem;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Inject
    private ProcessorContext initialProcessorContext;

    @Resource(name = "bulkMatchProcessorExecutor")
    private BulkMatchProcessorExecutor bulkMatchProcessorExecutor;

    @Resource(name = "bulkMatchProcessorAsyncExecutor")
    private BulkMatchProcessorExecutor bulkMatchProcessorAsyncExecutor;

    @Override
    public String process(DataCloudJobConfiguration jobConfiguration) throws Exception {
        try {
            LogManager.getLogger(MatchStepAspect.class).setLevel(Level.DEBUG);
            matchActorSystem.setBatchMode(true);
            setAllocateModeFlag(jobConfiguration.getMatchInput());

            initialProcessorContext.initialize(this, jobConfiguration);
            if (initialProcessorContext.isUseRemoteDnB()) {
                logger.info("Use async executor.");
                bulkMatchProcessorAsyncExecutor.execute(initialProcessorContext);
                bulkMatchProcessorAsyncExecutor.finalize(initialProcessorContext);
            } else {
                logger.info("Use sync executor.");
                bulkMatchProcessorExecutor.execute(initialProcessorContext);
                bulkMatchProcessorExecutor.finalize(initialProcessorContext);
            }

        } catch (Exception e) {
            String rootOperationUid = jobConfiguration.getRootOperationUid();
            String blockOperationUid = jobConfiguration.getBlockOperationUid();
            String errFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid)
                    .toString();
            try {
                HdfsUtils.writeToFile(yarnConfiguration, errFile, ExceptionUtils.getStackTrace(e));
            } catch (Exception e1) {
                logger.error("Failed to write error to err file.", e1);
            }
            throw (e);
        }

        return null;
    }

    private void setAllocateModeFlag(MatchInput input) {
        boolean allocateMode = OperationalMode.isEntityMatch(input.getOperationalMode())
                && (input.isAllocateId() || input.isFetchOnly());
        if (allocateMode) {
            if (input.isAllocateId()) {
                logger.info("Entity match is in Allocate mode which generates EntityId during match");
            } else if (input.isFetchOnly()) {
                logger.info("Entity match is in FetchOnly mode "
                        + "which looks up seed by EntityId in staging table or serving table (if no found in staging table)");
            }
            entityMatchConfigurationService.setIsAllocateMode(allocateMode);
        }
    }

}
