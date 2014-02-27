package com.latticeengines.perf.exposed.metric.sink;

import java.io.File;

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
            watchDir.mkdir();
            try {
                Process p = Runtime.getRuntime().exec("chmod 777 " + watchDir.getAbsolutePath());
                p.waitFor();
            } catch (Exception e) {
                throw new IllegalStateException("Cannot create directory " + watchDir.getAbsolutePath());
            }
            
        }
        super.init(conf);
    }

    @Override
    public void putMetrics(MetricsRecord record) {
        if (writeToFile()) {
            super.putMetrics(record);
        }
        
    }
    
    public boolean writeToFile() {
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
        return writeToFile;
    }

}
