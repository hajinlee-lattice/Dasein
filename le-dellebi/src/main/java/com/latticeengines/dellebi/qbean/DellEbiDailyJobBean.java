package com.latticeengines.dellebi.qbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.impl.DellEbiDailyJobCallable;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

/**
 * Created by lyan on 16-9-19.
 */
public abstract class DellEbiDailyJobBean implements QuartzJobBean {
    @Autowired
    protected DailyFlow dailyFlow;

    @Autowired
    protected ExportAndReportService exportAndReportService;

    @Autowired
    protected DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    protected String quartzJob = "dellEbiDailyJob";

    public Callable<Boolean> getCallable() {
        DellEbiDailyJobCallable.Builder builder = new DellEbiDailyJobCallable.Builder();
        builder.dailyFlow(dailyFlow).exportAndReportService(exportAndReportService)
                .dellEbiConfigEntityMgr(dellEbiConfigEntityMgr).quartzJob(quartzJob);
        return new DellEbiDailyJobCallable(builder);
    }

    protected void setQuartzJob(String quartzJob) {
        this.quartzJob = quartzJob;
    }
}
