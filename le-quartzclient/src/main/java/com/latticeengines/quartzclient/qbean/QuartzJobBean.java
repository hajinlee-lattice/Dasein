package com.latticeengines.quartzclient.qbean;

import java.util.concurrent.Callable;

public interface QuartzJobBean {

    Callable<Boolean> getCallable();
}
