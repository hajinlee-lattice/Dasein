package com.latticeengines.scoring.orchestration.service.impl;

import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandLogEntityMgr;
import com.latticeengines.scoring.orchestration.service.ScoringCommandLogService;

@Component("scoringCommandLogService")
public class ScoringCommandLogServiceImpl implements ScoringCommandLogService {

    public static final String SCORINGCOMMAND_ID_LOG_PREFIX = "LeadInputQueue_ID";
    public static final String SCORINGCOMMAND_CUSTOMER_ID_LOG_PREFIX = "LEDeployment_ID";
    private static final Log log = LogFactory.getLog(ScoringCommandLogServiceImpl.class);
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond();

    @Autowired
    private ScoringCommandLogEntityMgr scoringCommandLogEntityMgr;

    @Override
    public void log(ScoringCommand scoringCommand, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append(SCORINGCOMMAND_ID_LOG_PREFIX).append(":").append(scoringCommand.getPid()).append(" ,")
                .append(SCORINGCOMMAND_CUSTOMER_ID_LOG_PREFIX).append(":").append(scoringCommand.getId()).append(" ")
                .append(message);
        log.info(sb.toString());
        ScoringCommandLog scoringCommandLog = new ScoringCommandLog(scoringCommand, message);

        // Insert new row for each message
        scoringCommandLogEntityMgr.create(scoringCommandLog);
    }

    @Override
    public void logBeginStep(ScoringCommand scoringCommand, ScoringCommandStep step) {
        StringBuilder sb = new StringBuilder();
        sb.append(step.getDescription()).append(" submitted at ").append(dateTimeFormatter.print(new DateTime(DateTimeZone.UTC)));
        log(scoringCommand, sb.toString());
    }

    @Override
    public void logCompleteStep(ScoringCommand scoringCommand, ScoringCommandStep step, ScoringCommandStatus status) {
        StringBuilder sb = new StringBuilder();
        sb.append(step.getDescription()).append(" [").append(status).append("] ").append("completed at ")
                .append(dateTimeFormatter.print(new DateTime(DateTimeZone.UTC)));
        log(scoringCommand, sb.toString());
    }

    @Override
    public void logLedpException(ScoringCommand scoringCommand, LedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getFullStackTrace(e.getCause()) : "";
        StringBuffer sb = new StringBuffer();
        sb.append("errorCode:").append(e.getCode().name()).append(" errorMessage:").append(e.getMessage())
                .append(LINE_SEPARATOR).append(stackTrace);
        log(scoringCommand, sb.toString());
    }

    @Override
    public void logException(ScoringCommand scoringCommand, Exception e) {
        logException(scoringCommand, "", e);
    }

    @Override
    public void logException(ScoringCommand scoringCommand, String message, Exception e) {
        String stackTrace = e != null ? ExceptionUtils.getFullStackTrace(e) : "";
        StringBuffer sb = new StringBuffer(message);
        sb.append(" errorMessage:").append(e.getMessage()).append(LINE_SEPARATOR).append(stackTrace);
        log(scoringCommand, sb.toString());
    }

    @Override
    public void logYarnAppId(ScoringCommand scoringCommand, String yarnAppId, ScoringCommandStep step) {
        StringBuilder sb = new StringBuilder();
        sb.append(step.getDescription()).append(" [").append(yarnAppId).append("] ");
        log(scoringCommand, sb.toString());
    }

    @Override
    public List<ScoringCommandLog> findByScoringCommand(ScoringCommand scoringCommand) {
        return scoringCommandLogEntityMgr.findByScoringCommand(scoringCommand);
    }

}
