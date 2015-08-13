package com.latticeengines.release.exposed.activities;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.ProcessContext;
import com.latticeengines.release.exposed.domain.StatusContext;

public abstract class BaseActivity implements Activity {

    @Autowired
    protected ProcessContext processContext;

    @Autowired
    protected StatusContext statusContext;

    protected static final Log log = LogFactory.getLog(BaseActivity.class);

    protected ErrorHandler errorHandler;

    private String beanName;

    public abstract StatusContext runActivity();

    public BaseActivity(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public StatusContext execute() {
        try {
            return runActivity();
        } catch (Exception e) {
            if (errorHandler != null) {
                errorHandler.handleError(e);
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
