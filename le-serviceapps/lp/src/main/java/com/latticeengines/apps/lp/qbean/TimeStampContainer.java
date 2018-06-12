package com.latticeengines.apps.lp.qbean;

import org.apache.commons.net.ntp.TimeStamp;
import org.springframework.stereotype.Component;

@Component("timeStampContainer")
public class TimeStampContainer {

    private TimeStamp ts = TimeStamp.getCurrentTime();

    public void setTimeStamp(){
       ts = TimeStamp.getCurrentTime();
    }

    public TimeStamp getTimeStamp(){
       return this.ts;
    }
}
