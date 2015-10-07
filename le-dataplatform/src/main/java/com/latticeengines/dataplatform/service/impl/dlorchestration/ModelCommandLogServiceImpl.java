package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("modelCommandLogService")
public class ModelCommandLogServiceImpl implements ModelCommandLogService {

    public static final String MODELCOMMAND_ID_LOG_PREFIX = "ModelCommandId";
    private static final Log log = LogFactory.getLog(ModelCommandLogServiceImpl.class);
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond();

    @Autowired
    private ModelCommandLogEntityMgr modelCommandLogEntityMgr;

    @Override
    public void log(ModelCommand modelCommand, String message) {
        log.info(MODELCOMMAND_ID_LOG_PREFIX + ":" + modelCommand.getPid() + " " + message);

        ModelCommandLog modelCommandLog = new ModelCommandLog(modelCommand, message);

        // Insert new row for each message
        modelCommandLogEntityMgr.create(modelCommandLog);
    }

    @Override
    public void logBeginStep(ModelCommand modelCommand, ModelCommandStep step) {
        StringBuilder sb = new StringBuilder();
        if (step == null) {
            sb.append("Step is null");
        } else {
            sb.append(step.getDescription()).append(" submitted at ").append(dateTimeFormatter.print(new DateTime()));
        }
        
        log(modelCommand, sb.toString());
    }

    @Override
    public void logCompleteStep(ModelCommand modelCommand, ModelCommandStep step, ModelCommandStatus status) {
        StringBuilder sb = new StringBuilder();
        if (step == null) {
            sb.append("Step is null");
        } else {
            sb.append(step.getDescription()) //
              .append(" [").append(status) //
              .append("] ").append("completed at ") //
              .append(dateTimeFormatter.print(new DateTime()));
        }
        log(modelCommand, sb.toString());
    }

    @Override
    public void logLedpException(ModelCommand modelCommand, LedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getFullStackTrace(e.getCause()) : "";
        StringBuffer sb = new StringBuffer();
        sb.append("errorCode:").append(e.getCode().name()).append(" errorMessage:").append(e.getMessage())
                .append(LINE_SEPARATOR).append(stackTrace);
        log(modelCommand, sb.toString());
    }

    @Override
    public void logException(ModelCommand modelCommand, Exception e) {
        logException(modelCommand, "", e);
    }

    @Override
    public void logException(ModelCommand modelCommand, String message, Exception e) {
        String stackTrace = e != null ? ExceptionUtils.getFullStackTrace(e) : "";
        StringBuffer sb = new StringBuffer(message);
        sb.append(" errorMessage:").append(e.getMessage()).append(LINE_SEPARATOR).append(stackTrace);
        log(modelCommand, sb.toString());
    }

    @Override
    public void logYarnAppId(ModelCommand modelCommand, String yarnAppId, ModelCommandStep step) {
        StringBuilder sb = new StringBuilder();
        sb.append(step.getDescription()).append(" [").append(yarnAppId).append("] ");
        log(modelCommand, sb.toString());
    }

    @Override
    public List<ModelCommandLog> findByModelCommand(ModelCommand modelCommand) {
        return modelCommandLogEntityMgr.findByModelCommand(modelCommand);
    }

}
