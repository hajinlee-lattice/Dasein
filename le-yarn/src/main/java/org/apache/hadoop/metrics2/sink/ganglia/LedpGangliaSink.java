package org.apache.hadoop.metrics2.sink.ganglia;

import java.io.IOException;


public class LedpGangliaSink extends GangliaSink31 {

    private static final int DEFAULT_DMAX = 8;
    
    @Override
    protected void emitMetric(String groupName, String name, String type,
            String value, GangliaConf gConf, GangliaSlope gSlope) throws IOException {
        if (name.contains("ledpjob")) {
            gConf.setDmax(DEFAULT_DMAX);
        }
        super.emitMetric(groupName, name, type, value, gConf, gSlope);
    }

}
