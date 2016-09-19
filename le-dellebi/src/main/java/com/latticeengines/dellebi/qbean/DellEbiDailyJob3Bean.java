package com.latticeengines.dellebi.qbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.impl.DellEbiDailyJobCallable;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("dellEbiDailyJob3")
public class DellEbiDailyJob3Bean extends DellEbiDailyJobBean {

    private final String quartzJob = "dellEbiDailyJob3";

    @Override
    public Callable<Boolean> getCallable() {
        super.setQuartzJob(quartzJob);
        return super.getCallable();
    }
}
