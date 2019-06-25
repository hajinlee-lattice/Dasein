package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.DeltaCalculationService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;

@Component("deltaCalculationService")
public class DeltaCalculationServiceImpl implements DeltaCalculationService {
    private static final Logger log = LoggerFactory.getLogger(DeltaCalculationServiceImpl.class);

    @Inject
    PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    private static int count = 1;

    public Boolean triggerScheduledCampaigns() {
        log.info("Delta calc running here : " + count);
        List<Callable<String>> fileExporters = new ArrayList<>();
        Date fileExportTime = new Date();
        // fileExporters.add(new CsvFileExporter(yarnConfiguration, config, recAvroHdfsFilePath, fileExportTime));
        // fileExporters.add(new JsonFileExporter(yarnConfiguration, config, recAvroHdfsFilePath, fileExportTime));

        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("playlaunch-export", 2);
        List<String> exportFiles = ThreadPoolUtils.runCallablesInParallel(executorService, fileExporters,
                (int) TimeUnit.DAYS.toMinutes(1), 30);

        return null;
    }
}
