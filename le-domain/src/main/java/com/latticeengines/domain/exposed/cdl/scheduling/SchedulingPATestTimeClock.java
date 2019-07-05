package com.latticeengines.domain.exposed.cdl.scheduling;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulingPATestTimeClock implements TimeClock {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPATestTimeClock.class);

    private Date date;

    public SchedulingPATestTimeClock(String formatdate) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            this.date = simpleDateFormat.parse(formatdate);
        } catch (ParseException e) {
            log.info("can not parse the String to date: " + e.getMessage());
        }
    }

    @Override
    public long getCurrentTime() {
        return date.getTime();
    }
}
