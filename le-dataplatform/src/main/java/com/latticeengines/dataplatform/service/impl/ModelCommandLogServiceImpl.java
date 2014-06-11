package com.latticeengines.dataplatform.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.service.ModelCommandLogService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

@Component("modelCommandLogService")
public class ModelCommandLogServiceImpl implements ModelCommandLogService {

    private static final Log log = LogFactory.getLog(ModelCommandLogServiceImpl.class);
    private static final Map<ModelCommandStep, String> stepToDescription;
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond();

    @Autowired
    private ModelCommandLogEntityMgr modelCommandLogEntityMgr;

    static {
        Map<ModelCommandStep, String> aStepToDescription = new HashMap<>();
        aStepToDescription.put(ModelCommandStep.LOAD_DATA, "Load data");
        aStepToDescription.put(ModelCommandStep.GENERATE_SAMPLES, "Generate samples");
        aStepToDescription.put(ModelCommandStep.SUBMIT_MODELS, "Submit models");
        aStepToDescription.put(ModelCommandStep.SEND_JSON, "Submit JSON to BARD");
        stepToDescription = Collections.unmodifiableMap(aStepToDescription);
    }

    @Override
    public void log(int commandId, String message) {
        log.info("ModelCommandId:" + commandId + " " + message);

        ModelCommandLog modelCommandLog = new ModelCommandLog();
        modelCommandLog.setCommandId(commandId);
        modelCommandLog.setMessage(message);

        // Insert new row for each message
        ///modelCommandLogEntityMgr.post(modelCommandLog);
        ///modelCommandLogEntityMgr.save();
    }

    @Override
    public void logBeginStep(int commandId, ModelCommandStep step) {
        StringBuilder sb = new StringBuilder();
        sb.append(stepToDescription.get(step)).append(" submitted at ").append(dateTimeFormatter.print(new DateTime()));
        log(commandId, sb.toString());
    }

    @Override
    public void logCompleteStep(int commandId, ModelCommandStep step, ModelCommandStatus status) {
        StringBuilder sb = new StringBuilder();
        sb.append(stepToDescription.get(step)).append(" [").append(status).append("] ").append("completed at ")
                .append(dateTimeFormatter.print(new DateTime()));
        log(commandId, sb.toString());
    }

    @Override
    public void logLedpException(int commandId, LedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getFullStackTrace(e.getCause()) : "";
        StringBuffer sb = new StringBuffer();
        sb.append("errorCode:").append(e.getCode().name()).append(" errorMessage:").append(e.getMessage())
                .append(LINE_SEPARATOR).append(stackTrace);
        log(commandId, sb.toString());
    }

    @Override
    public void logException(int commandId, Exception e) {
        String stackTrace = e != null ? ExceptionUtils.getFullStackTrace(e) : "";
        StringBuffer sb = new StringBuffer();
        sb.append("errorMessage:").append(e.getMessage()).append(LINE_SEPARATOR).append(stackTrace);
        log(commandId, sb.toString());
    }
}
