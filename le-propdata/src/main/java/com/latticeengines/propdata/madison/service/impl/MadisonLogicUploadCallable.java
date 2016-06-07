package com.latticeengines.propdata.madison.service.impl;

import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.propdata.madison.service.PropDataContext;
import com.latticeengines.propdata.madison.service.PropDataMadisonService;

public class MadisonLogicUploadCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(MadisonLogicUploadCallable.class);
    private PropDataMadisonService propDataMadisonService;
    private boolean propdataJobsEnabled;

    public MadisonLogicUploadCallable(Builder builder) {
        this.propDataMadisonService = builder.getPropDataMadisonService();
        this.propdataJobsEnabled = builder.getPropdataJobsEnabled();
    }

    @Override
    public Boolean call() throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            log.info("Started!");
            if (propdataJobsEnabled == false) {
                log.info("Job is disabled");
                return false;
            }
            PropDataContext requestContextForTransformaton = new PropDataContext();
            PropDataContext responseContextForTransformaton = propDataMadisonService
                    .transform(requestContextForTransformaton);
            if (responseContextForTransformaton.getProperty(PropDataMadisonService.STATUS_KEY,
                    String.class) == null
                    || !responseContextForTransformaton.getProperty(
                            PropDataMadisonService.STATUS_KEY, String.class)
                            .equals(PropDataMadisonService.STATUS_OK)) {
                log.info("Finished! Upload job has no transformation.");
                return true;
            }

            Date today = responseContextForTransformaton.getProperty(
                    PropDataMadisonService.TODAY_KEY, Date.class);
            PropDataContext requestContextForUpload = new PropDataContext();
            requestContextForUpload.setProperty(PropDataMadisonService.TODAY_KEY, today);
            propDataMadisonService.exportToDB(requestContextForUpload);

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
