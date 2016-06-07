package com.latticeengines.propdata.madison.service.impl;

import java.util.concurrent.Callable;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.propdata.madison.service.PropDataContext;
import com.latticeengines.propdata.madison.service.PropDataMadisonService;

public class MadisonLogicDownloadCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(MadisonLogicDownloadCallable.class);
    private PropDataMadisonService propDataMadisonService;
    private boolean propdataJobsEnabled;

    public MadisonLogicDownloadCallable(Builder builder) {
        this.propDataMadisonService = builder.getPropDataMadisonService();
        this.propdataJobsEnabled = builder.getPropdataJobsEnabled();
    }

    @SuppressWarnings("unused")
    @Override
    public Boolean call() throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            log.info("Started!");
            if (propdataJobsEnabled == false) {
                log.info("Job is disabled");
                return false;
            }
            PropDataContext requestContext = new PropDataContext();
            PropDataContext responseContext = propDataMadisonService.importFromDB(requestContext);

            long endTime = System.currentTimeMillis();
            log.info("Finished! Eclipsed time="
                    + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));

        } catch (Exception ex) {
            log.error("Failed!", ex);
        }
        return true;
    }

    public static class Builder {

        private PropDataMadisonService propDataMadisonService;
        private boolean propdataJobsEnabled;

        public Builder() {

        }

        public Builder propDataMadisonService(PropDataMadisonService propDataMadisonService) {
            this.propDataMadisonService = propDataMadisonService;
            return this;
        }

        public Builder propdataJobsEnabled(Boolean propdataJobsEnabled) {
            this.propdataJobsEnabled = propdataJobsEnabled;
            return this;
        }

        public PropDataMadisonService getPropDataMadisonService() {
            return propDataMadisonService;
        }

        public boolean getPropdataJobsEnabled() {
            return propdataJobsEnabled;
        }
    }

}
