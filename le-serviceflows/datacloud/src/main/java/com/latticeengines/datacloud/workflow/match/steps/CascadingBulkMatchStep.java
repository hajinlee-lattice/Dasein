package com.latticeengines.datacloud.workflow.match.steps;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CascadingBulkMatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("cascadingBulkMatchStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CascadingBulkMatchStep extends RunDataFlow<CascadingBulkMatchStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CascadingBulkMatchStep.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MatchCommandService matchCommandService;

    @Override
    public void execute() {
        try {
            log.info("Starting CascadingBulkMatchStep execute()");
            super.execute();
            writeSuccessMatchCommand();
            log.info("End CascadingBulkMatchStep execute()");
        } catch (Exception ex) {
            writeFailedMatchCommand(ex);
            throw ex;
        }
    }

    private void writeSuccessMatchCommand() {
        String rootOperationUid = configuration.getRootOperationUid();
        Long count = AvroUtils.count(yarnConfiguration, configuration.getTargetPath() + "/*.avro");
        matchCommandService.update(rootOperationUid) //
                .resultLocation(configuration.getTargetPath()) //
                .status(MatchStatus.FINISHED) //
                .progress(1f) //
                .rowsMatched(count.intValue()) //
                .commit();
    }

    private void writeFailedMatchCommand(Exception ex) {
        String rootOperationUid = configuration.getRootOperationUid();
        matchCommandService.update(rootOperationUid) //
                .status(MatchStatus.FAILED) //
                .errorMessage(ex.getMessage()) //
                .commit();
    }

}
