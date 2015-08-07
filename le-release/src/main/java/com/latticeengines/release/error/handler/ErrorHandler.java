package com.latticeengines.release.error.handler;

import com.latticeengines.release.exposed.domain.ProcessContext;

public interface ErrorHandler {

    public void handleError(ProcessContext context, Throwable th);

}
