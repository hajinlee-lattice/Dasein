package com.latticeengines.perf.exposed.metric.sink;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.sink.FileSink;

public class SwitchableFileSink extends FileSink {
    public static final String START_FILENAME = "START";
    public static final String STOP_FILENAME = "STOP";
    private static final String WATCHDIR_KEY = "watchdir";
    private static File watchDir;

    @Override
    public void init(SubsetConfiguration conf) {
        String watchDirStr = conf.getString(WATCHDIR_KEY); 
        watchDir = new File(watchDirStr);
        if (!watchDir.exists()) {
            try {
                watchDir.createNewFile();
            } catch (IOException e) {
                throw new IllegalStateException("Cannot create watch directory " + watchDirStr + ".");
            }
        }
    }

    @Override
    public void putMetrics(MetricsRecord record) {
        String[] files = watchDir.list();
        boolean writeToFile = false;
        
        for (String file : files) {
            if (file.equals(START_FILENAME)) {
                writeToFile = true;
            }
            if (file.equals(STOP_FILENAME)) {
                writeToFile = false;
            }
        }
        
        if (writeToFile) {
            super.putMetrics(record);
        }
        
    }

}
