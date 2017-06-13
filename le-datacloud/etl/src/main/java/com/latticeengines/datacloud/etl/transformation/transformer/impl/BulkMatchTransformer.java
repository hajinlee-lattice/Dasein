package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

@Component(TRANSFORMER_MATCH)
public class BulkMatchTransformer extends AbstractMatchTransformer {

    @Autowired
    protected MatchProxy matchProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    private static final Log log = LogFactory.getLog(AbstractTransformer.class);
    private static String transfomerName = TRANSFORMER_MATCH;

    public String getName() {
        return transfomerName;
    }

    @Override
    protected boolean match(String inputAvroPath, String outputAvroPath, MatchTransformerConfig config) {
        MatchInput input = constructMatchInput(inputAvroPath, config);
        MatchCommand matchCommand = matchProxy.matchBulk(input, HdfsPodContext.getHdfsPodId());
        matchCommand = waitForMatchCommand(matchCommand);
        return saveResult(matchCommand, outputAvroPath);
    }

    private MatchInput constructMatchInput(String avroDir, MatchTransformerConfig config) {
        MatchInput matchInput = config.getMatchInput();
        if (matchInput.getTenant() == null) {
            matchInput.setTenant(new Tenant(PropDataConstants.SERVICE_CUSTOMERSPACE));
        }
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);
        return matchInput;
    }

    private MatchCommand waitForMatchCommand(MatchCommand matchCommand) {
        String rootUid = matchCommand.getRootOperationUid();
        log.info(String.format("Waiting for match command %s to complete", rootUid));

        MatchStatus status;
        do {
            matchCommand = matchProxy.bulkMatchStatus(rootUid);
            status = matchCommand.getMatchStatus();
            if (status == null) {
                throw new LedpException(LedpCode.LEDP_28024, new String[] { rootUid });
            }
            String logMsg = "Match Status = " + status;
            if (MatchStatus.MATCHING.equals(status)) {
                Float progress = matchCommand.getProgress();
                logMsg += String.format(": %.2f %%", progress * 100);
            }
            log.info(logMsg);

            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                // Ignore InterruptedException
            }

        } while (!status.isTerminal());

        if (!MatchStatus.FINISHED.equals(status)) {
            IllegalStateException inner = new IllegalStateException("The terminal status of match is " + status
                    + " instead of " + MatchStatus.FINISHED);
            throw new LedpException(LedpCode.LEDP_00006, inner);
        }
        return matchCommand;
    }

    private boolean saveResult(MatchCommand matchCommand, String resultDir) {
        String rootUid = matchCommand.getRootOperationUid();
        String outputDir = hdfsPathBuilder.constructMatchOutputDir(rootUid).toString();
        try {
            String avroGlobs = outputDir + (outputDir.endsWith("/") ? "*.avro" : "/*.avro");
            for (String avroFilePath : HdfsUtils.getFilesByGlob(yarnConfiguration, avroGlobs)) {
                String avroFileName = new Path(avroFilePath).getName();
                log.info("Move file from " + avroFilePath + " to " + new Path(resultDir, avroFileName).toString());
                HdfsUtils.moveFile(yarnConfiguration, avroFilePath, new Path(resultDir, avroFileName).toString());
            }
        } catch (Exception e) {
            log.error("Failed to save match result", e);
            return false;
        }
        return true;
    }
}
