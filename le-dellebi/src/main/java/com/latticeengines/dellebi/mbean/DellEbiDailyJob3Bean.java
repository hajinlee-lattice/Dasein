package com.latticeengines.dellebi.mbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.impl.DellEbiDailyJobCallable;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;

@Component("dellEbiDailyJob3")
public class DellEbiDailyJob3Bean implements QuartzJobBean {

    @Autowired
    private DailyFlow dailyFlow;
    
    @Autowired
    private ExportAndReportService exportAndReportService;
    
    @Value("${dellebi.fileTypes.dellebiManagerJob3}")
    private String fileTypesList;

    @Override
    public Callable<Boolean> getCallable() {
        DellEbiDailyJobCallable.Builder builder = new DellEbiDailyJobCallable.Builder();
        builder.dailyFlow(dailyFlow)
                .exportAndReportService(exportAndReportService)
                .fileTypesList(fileTypesList);
        return new DellEbiDailyJobCallable(builder);
    }

}
