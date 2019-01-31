package com.latticeengines.datacloud.workflow.match.steps;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CleanupBulkEntityMatchConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

/**
 * Cleanup step for bulk entity match workflow
 */
@Component("cleanupBulkEntityMatch")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CleanupBulkEntityMatch extends BaseWorkflowStep<CleanupBulkEntityMatchConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CleanupBulkEntityMatch.class);

    @Override
    public void execute() {
        String testDir = getConfiguration().getTmpDir();
        if (StringUtils.isNotBlank(testDir)) {
            log.info("Cleanup test directory ({}) for entity match", testDir);
            try {
                HdfsUtils.rmdir(yarnConfiguration, testDir);
            } catch (IOException e) {
                log.error("Failed to remove test directory ({}), error = {}", testDir, e);
            }
        }
    }
}
