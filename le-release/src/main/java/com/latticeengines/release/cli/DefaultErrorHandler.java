package com.latticeengines.release.cli;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.ProcessContext;

@Component("defaultErrorHandler")
public class DefaultErrorHandler implements ErrorHandler{

    private static final Log log = LogFactory.getLog(DefaultErrorHandler.class);

    @Override
    public void handleError(ProcessContext context, Throwable th) {
        log.error(th.getMessage());
        log.error(ExceptionUtils.getStackTrace(th));
    }

}
