package com.latticeengines.dellebi.util;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;

public class HadoopFileSystemOperations{
    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;
    
    @Value("${dellebi.datahadoopinpath}")
    private String dataHadoopInPath;
    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;
    
    @Value("${dellebi.quotetrans}")
    private String quoteTrans;
    
    private boolean ifFolderExist;

    private static final Log log = LogFactory.getLog(HadoopFileSystemOperations.class);

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

    public int listFileNumber(String folderName) {
        int fileNumber = 0;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(folderName);
            FileStatus[] status = fs.listStatus(path);
            for (FileStatus s : status) {
                String name = s.getPath().getName();
                if (name.endsWith(".txt")) {
                    log.info("Cascading found txt file: " + name + " and starts to process it.");
                    fileNumber++;
                }
            }

        } catch (Exception e) {
            log.warn("Failed!", e);
        }
        return fileNumber;
    }

    public boolean isEmpty(String folderName) {
        if (listFileNumber(folderName) == 0) {
            return true;
        } else {
            return false;
        }
    }

    public void isExistWithoutReturnValue(String folderName) {
    	
    	boolean isExist = false;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(folderName);
            ifFolderExist = fs.exists(path);
               
        } catch (Exception e) {
            log.warn("Failed to check if " + folderName + "exists or not.", e);
        }

        ifFolderExist = isExist;
    }
    
public boolean isExist(String folderName) {
    	
    	boolean isExist = false;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(folderName);
            return fs.exists(path);
               
        } catch (Exception e) {
            log.warn("Failed to check if " + folderName + "exists or not.", e);
        }

        return isExist;
    }

public boolean isExistWithOpenFileName(String folderName) {

    try {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
        Path path = new Path(folderName);
        if(fs.exists(path)){
        	FileStatus[] fileStatus = fs.listStatus(path);
            
            for(FileStatus status:fileStatus ){
            	if(status.getPath().getName().endsWith(".txt.opened")){
            		return true;
            	}
            }
        }          
    } catch (Exception e) {
        log.warn("Failed to check if " + folderName + "exists or not.", e);
    }

    return false;
}

    public void removeFile(String fileName) {

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(fileName);
            fs.deleteOnExit(path);
        } catch (Exception e) {
            log.warn("Failed!", e);
        }
    }
    
    public Boolean getIfNotExist(){
    	return !ifFolderExist;
    }
    
    public Boolean ifReadyToProcessData(){
    	if(listFileNumber(dataHadoopInPath + "/" + quoteTrans) != 0
    			&& isExist(dataHadoopWorkingPath + "/" + quoteTrans) != true
    			&& isExistWithOpenFileName(dataHadoopInPath + "/" + quoteTrans) != true){
    		log.info("Cascading meets the conditions to process data from in folder on HDFS.");
    		return true;
    	}
    	log.info("Cascading doesn't meet the conditions to process data from in folder on HDFS.");
    	return false;
    }
}
