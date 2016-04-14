package com.latticeengines.workflow.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("failableStep")
public class FailableStep extends AbstractStep<BaseStepConfiguration> {

    private static final Log log = LogFactory.getLog(FailableStep.class);

    private boolean fail = true;
    private boolean useRuntimeException = false;

    @Override
    public void execute() {
        log.info("Inside FailableStep execute()");
        if (fail) {
            if (useRuntimeException) {
                throw new RuntimeException("Simulated failure!");
            }
            throw new LedpException(LedpCode.LEDP_28001, new String[] { "Simulated failure!" });
        }
    }

    public boolean isFail() {
        return fail;
    }

    public void setFail(boolean fail) {
        this.fail = fail;
    }

    public boolean useRuntimeException() {
        return useRuntimeException;
    }

    public void setUseRuntimeException(boolean useRuntimeException) {
        this.useRuntimeException = useRuntimeException;
    }
}
