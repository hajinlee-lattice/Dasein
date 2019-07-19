package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.io.IOException;
import java.util.Random;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component(TRANSFORMER_MATCH)
public class BulkMatchTransformer extends AbstractMatchTransformer {

    private final MetadataProxy metadataProxy;
    private final MatchProxy matchProxy;
    protected final Configuration yarnConfiguration;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    private static final Logger log = LoggerFactory.getLogger(BulkMatchTransformer.class);

    @Inject
    public BulkMatchTransformer(MetadataProxy metadataProxy, MatchProxy matchProxy, Configuration yarnConfiguration) {
        this.metadataProxy = metadataProxy;
        this.matchProxy = matchProxy;
        this.yarnConfiguration = yarnConfiguration;
    }

    public String getName() {
        return TRANSFORMER_MATCH;
    }

    @Override
    protected MatchCommand match(String inputAvroPath, String outputAvroPath, MatchTransformerConfig config) {
        MatchInput input = constructMatchInput(inputAvroPath, config);
        return match(input, outputAvroPath, config);
    }

    @Override
    protected MatchCommand match(String inputAvroPath, Schema schema, String outputAvroPath,
            MatchTransformerConfig config) {
        log.info("Using table schema: " + schema.toString(true));
        MatchInput input = constructMatchInput(inputAvroPath, schema, config);
        return match(input, outputAvroPath, config);
    }

    private MatchCommand match(MatchInput input, String outputAvroPath, MatchTransformerConfig config) {
        MatchCommand matchCommand = matchProxy.matchBulk(input, HdfsPodContext.getHdfsPodId());
        matchCommand = waitForMatchCommand(matchCommand);
        saveResult(matchCommand, outputAvroPath);
        if (input.isOutputNewEntities() && StringUtils.isNotBlank(config.getNewEntitiesTableName())) {
            String customer = CustomerSpace.shortenCustomerSpace(input.getTenant().getId());
            registerNewEntitiesTable(customer, matchCommand, config.getNewEntitiesTableName());
        }
        return matchCommand;
    }

    private MatchInput constructMatchInput(String avroDir, MatchTransformerConfig config) {
        return constructMatchInput(avroDir, null, config);
    }

    private MatchInput constructMatchInput(String avroDir, Schema schema, MatchTransformerConfig config) {
        MatchInput matchInput = config.getMatchInput();
        if (matchInput.getTenant() == null) {
            matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_CUSTOMERSPACE));
        }
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        if (schema != null) {
            inputBuffer.setSchema(schema);
        }
        matchInput.setInputBuffer(inputBuffer);
        if (!OperationalMode.isEntityMatch(matchInput.getOperationalMode())) {
            matchInput.setDataCloudOnly(true);
        }
        return matchInput;
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
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                // Ignore InterruptedException
            }
        } while (!status.isTerminal());
        LogManager.getLogger(BaseRestApiProxy.class).setLevel(level);

        if (!MatchStatus.FINISHED.equals(status)) {
            IllegalStateException inner = new IllegalStateException(
                    "The terminal status of match is " + status + " instead of " + MatchStatus.FINISHED);
            throw new LedpException(LedpCode.LEDP_00006, inner);
        }
        return matchCommand;
    }

    private void saveResult(MatchCommand matchCommand, String resultDir) {
        String rootUid = matchCommand.getRootOperationUid();
        String outputDir = hdfsPathBuilder.constructMatchOutputDir(rootUid).toString();
        try {
            String avroGlobs = PathUtils.toAvroGlob(outputDir);
            log.info(String.format("Moving files from %s to %s", avroGlobs, resultDir));
            int cnt = 0;
            for (String avroFilePath : HdfsUtils.getFilesByGlob(yarnConfiguration, avroGlobs)) {
                String avroFileName = new Path(avroFilePath).getName();
                HdfsUtils.moveFile(yarnConfiguration, avroFilePath, new Path(resultDir, avroFileName).toString());
                cnt++;
            }
            log.info(String.format("Moved %d files from %s to %s", cnt, avroGlobs, resultDir));
        } catch (Exception e) {
            log.error("Failed to save match result", e);
        }
    }

    private void registerNewEntitiesTable(String customer, MatchCommand matchCommand, String targetTableName) {
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
