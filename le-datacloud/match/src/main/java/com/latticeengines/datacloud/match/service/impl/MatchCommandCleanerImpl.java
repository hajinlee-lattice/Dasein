package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.entitymgr.MatchCommandEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandCleaner;
import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

@Component("matchCommandCleaner")
public class MatchCommandCleanerImpl implements MatchCommandCleaner {
    private static final Logger log = LoggerFactory.getLogger(MatchCommandCleanerImpl.class);

    @Value("${datacloud.match.retention.days}")
    private int retentionDays;

    @Inject
    private MatchCommandEntityMgr matchCommandEntityMgr;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

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
            log.info("Remove command:" + command.getRootOperationUid());
            List<MatchBlock> blocks = matchCommandEntityMgr.findBlocks(command.getRootOperationUid());
            removeMatchBlocks(command.getRootOperationUid(), blocks);
            matchCommandEntityMgr.deleteCommand(command);
            String path = command.getResultLocation();
            removeHdfsPath(path);
        }
    }

    private void removeMatchBlocks(String rootUid, List<MatchBlock> blocks) {
        for (MatchBlock block: blocks) {
            log.info("Removing match block " + block.getBlockOperationUid() + " belonging to " + rootUid + " from hdfs.");
            String blockDir = hdfsPathBuilder.constructMatchBlockDir(rootUid, block.getBlockOperationUid()).toString();
            removeHdfsPath(blockDir);
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
