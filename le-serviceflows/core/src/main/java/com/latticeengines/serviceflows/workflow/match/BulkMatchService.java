package com.latticeengines.serviceflows.workflow.match;

import java.io.IOException;
import java.util.Random;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Lazy
@Service("bulkMatchService")
public class BulkMatchService {

    private static final Logger log = LoggerFactory.getLogger(BulkMatchService.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private MatchProxy matchProxy;

    @Inject
    protected Configuration yarnConfiguration;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    public MatchCommand match(MatchInput input, String rootOperationUid) {
        MatchCommand matchCommand = StringUtils.isNotBlank(rootOperationUid)
                ? matchProxy.matchBulk(input, podId, rootOperationUid)
                : matchProxy.matchBulk(input, podId);
        matchCommand = waitForMatchCommand(matchCommand);
        return matchCommand;
    }

    private MatchCommand waitForMatchCommand(MatchCommand matchCommand) {
        String rootUid = matchCommand.getRootOperationUid();
        log.info(String.format("Waiting for match command %s to complete (%s)", rootUid,
                matchCommand.getApplicationId()));

        Random random = new Random(System.currentTimeMillis());
        Level level = LogManager.getLogger(BaseRestApiProxy.class).getLevel();
        LogManager.getLogger(BaseRestApiProxy.class).setLevel(Level.ERROR);
        MatchStatus status;
        do {
            matchCommand = matchProxy.bulkMatchStatus(rootUid);
            status = matchCommand.getMatchStatus();
            if (status == null) {
                throw new LedpException(LedpCode.LEDP_28024, new String[] { rootUid });
            }
            if (status.isTerminal() || random.nextInt(100) > 95) {
                String logMsg = "Match Status = " + status;
                if (MatchStatus.MATCHING.equals(status)) {
                    Float progress = matchCommand.getProgress();
                    logMsg += String.format(": %.2f %%", progress * 100);
                }
                log.info(logMsg);
            }
            SleepUtils.sleep(5000L);
        } while (!status.isTerminal());
        LogManager.getLogger(BaseRestApiProxy.class).setLevel(level);

        if (!MatchStatus.FINISHED.equals(status)) {
            IllegalStateException inner = new IllegalStateException(
                    "The terminal status of match is " + status + " instead of " + MatchStatus.FINISHED);
            throw new LedpException(LedpCode.LEDP_00006, inner);
        }
        return matchCommand;
    }

    public void registerResultTable(String customer, MatchCommand matchCommand, String targetTableName) {
        String outputDir = PathUtils.toParquetOrAvroDir(matchCommand.getResultLocation());
        String avroGlobs = PathUtils.toAvroGlob(outputDir);
        try {
            String tgtPath = PathBuilder.buildDataTablePath(podId, CustomerSpace.parse(customer))
                    .append(targetTableName).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, tgtPath)) {
                HdfsUtils.rmdir(yarnConfiguration, tgtPath);
            }
            HdfsUtils.mkdir(yarnConfiguration, tgtPath);
            int cnt = 0;
            for (String avroFilePath : HdfsUtils.getFilesByGlob(yarnConfiguration, avroGlobs)) {
                String avroFileName = new Path(avroFilePath).getName();
                String tgtFilePath = tgtPath + "/" + avroFileName;
                HdfsUtils.moveFile(yarnConfiguration, avroFilePath, tgtFilePath);
                log.info("Moved {} to {}", avroFilePath, tgtFilePath);
                cnt++;
            }
            log.info(String.format("Moved %d files from %s to %s", cnt, avroGlobs, tgtPath));
            Table table = MetadataConverter.getTable(yarnConfiguration, tgtPath, //
                    null, null, false);
            table.setName(targetTableName);
            metadataProxy.createTable(customer, targetTableName, table);
        } catch (Exception e) {
            log.error("Failed to save match result", e);
        }
    }

    public void registerNewEntitiesTable(String customer, MatchCommand matchCommand, String targetTableName) {
        String avroDir = PathUtils.toParquetOrAvroDir(matchCommand.getNewEntitiesLocation());
        String avroGlobs = PathUtils.toAvroGlob(avroDir);
        if (AvroUtils.hasRecords(yarnConfiguration, avroGlobs)) {
            log.info("Register avros in " + avroDir + " as table " + targetTableName);
            String tgtPath = PathBuilder.buildDataTablePath(podId, CustomerSpace.parse(customer))
                    .append(targetTableName).toString();
            try {
                log.info(String.format("Moving %s to %s", avroDir, tgtPath));
                HdfsUtils.moveFile(yarnConfiguration, avroDir, tgtPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to move data from " + avroGlobs + " to " + tgtPath);
            }
            Table table = MetadataConverter.getTable(yarnConfiguration, tgtPath, //
                    null, null, false);
            table.setName(targetTableName);
            metadataProxy.createTable(customer, targetTableName, table);
        } else {
            log.warn("There is no records for new entities, skip registering table " + targetTableName);
        }
    }

}
