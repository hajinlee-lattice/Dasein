package com.latticeengines.release.exposed.activities;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.ProcessContext;

public abstract class BaseActivity implements Activity {

    protected ErrorHandler errorHandler;

    private String beanName;

    protected static final Log log = LogFactory.getLog(BaseActivity.class);

    public abstract ProcessContext runActivity(ProcessContext context);

    public BaseActivity(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public ProcessContext execute(ProcessContext context) {
        try {
            return runActivity(context);
        } catch (Exception e) {
            if (errorHandler != null) {
                errorHandler.handleError(context, e);
            } else {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public String getBeanName() {
        return beanName;
    }

}
