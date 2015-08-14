package com.latticeengines.release.error.handler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.latticeengines.release.exposed.domain.ProcessContext;

@Component("defaultErrorHandler")
public class DefaultErrorHandler implements ErrorHandler {

    @Autowired
    private ProcessContext processContext;

    private static final Log log = LogFactory.getLog(DefaultErrorHandler.class);

    @Override
    public void handleError(Throwable th) {
        log.error(th);
        log.error(ExceptionUtils.getStackTrace(th));
        throw new RuntimeException(th);
    }

}
