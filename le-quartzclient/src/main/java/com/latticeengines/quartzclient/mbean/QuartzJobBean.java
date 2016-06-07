package com.latticeengines.quartzclient.mbean;

import java.util.concurrent.Callable;

public interface QuartzJobBean {

    Callable<Boolean> getCallable();
}
