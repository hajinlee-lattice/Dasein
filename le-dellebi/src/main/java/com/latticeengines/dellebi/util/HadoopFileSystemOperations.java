package com.latticeengines.dellebi.util;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dellebi.flowdef.DailyFlow;

public class HadoopFileSystemOperations implements FileSystemOperations {
    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;

    private static final Log log = LogFactory.getLog(HadoopFileSystemOperations.class);

    @Override
    public void cleanFolder(String folderName) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(folderName);
            if (fs.exists(path)) {
            	log.info("Deleting " + folderName);
                fs.delete(path, true); // Delete existing Directory
            }
        } catch (Exception e) {
        	log.warn("Failed!", e);
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
                	log.info("Cascading found txt file: " + name + " and starts to process it.");
                    fileNumber++;
                }
            }

        } catch (Exception e) {
        	log.warn("Failed!", e);
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
