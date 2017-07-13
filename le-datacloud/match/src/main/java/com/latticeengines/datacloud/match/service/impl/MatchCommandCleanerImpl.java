package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.match.entitymgr.MatchCommandEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandCleaner;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

@Component("matchCommandCleaner")
public class MatchCommandCleanerImpl implements MatchCommandCleaner {
    private static final Logger log = LoggerFactory.getLogger(MatchCommandCleanerImpl.class);

    @Value("${datacloud.match.retention.days:7}")
    private int retentionDays;

    @Autowired
    private MatchCommandEntityMgr matchCommandEntityMgr;

    @Autowired
    protected Configuration yarnConfiguration;

    @Override
    public void clean() {
        log.info("Begin to clean the match commands history");
        List<MatchCommand> commands = retrieveOutDatedCommands(retentionDays);
        removeCommands(commands);
        log.info("Finished cleaning the match commands history");
    }

    private List<MatchCommand> retrieveOutDatedCommands(int retentionDays) {
        return matchCommandEntityMgr.findOutDatedCommands(retentionDays);
    }

    private void removeCommands(List<MatchCommand> commands) {
        for(MatchCommand command : commands) {
            log.debug("Remove command:" + command.getRootOperationUid());
            matchCommandEntityMgr.deleteCommand(command);
            String path = command.getResultLocation();
            removeHdfsPath(path);
        }
    }

    private void removeHdfsPath(String path) {
        try {
            HdfsUtils.rmdir(yarnConfiguration, path);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
