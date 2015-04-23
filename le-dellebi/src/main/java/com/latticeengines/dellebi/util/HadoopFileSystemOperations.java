package com.latticeengines.dellebi.util;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dellebi.flowdef.DailyFlow;

public class HadoopFileSystemOperations implements FileSystemOperations {
    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;

    private final static Logger LOGGER = Logger.getLogger(HadoopFileSystemOperations.class);

    @Override
    public void cleanFolder(String folderName) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(folderName);
            if (fs.exists(path)) {
                fs.delete(path, true); // Delete existing Directory
            }
        } catch (Exception e) {
            LOGGER.warn("Failed!", e);
        }

    }

    @Override
    public int listFileNumber(String folderName) {
        int fileNumber = 0;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(folderName);
            FileStatus[] status = fs.listStatus(path);
            for (FileStatus s : status) {
                String name = s.getPath().getName();
                if( name.contains(".txt")){
                    LOGGER.info("Cascading found txt file: " + name + " and starts to process it.");
                    fileNumber++;
                }
            }

        } catch (Exception e) {
            LOGGER.warn("Failed!", e);
        }
        return fileNumber;
    }

    @Override
    public boolean isEmpty(String folderName) {
        if (listFileNumber(folderName) == 0) {
            return true;
        } else {
            return false;
        }
    }
}
