package com.latticeengines.datacloud.workflow.match.steps;

import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CascadingBulkMatchStepConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("cascadingBulkMatchStep")
@Scope("prototype")
public class CascadingBulkMatchStep extends RunDataFlow<CascadingBulkMatchStepConfiguration> {

    private static final Log log = LogFactory.getLog(CascadingBulkMatchStep.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
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
