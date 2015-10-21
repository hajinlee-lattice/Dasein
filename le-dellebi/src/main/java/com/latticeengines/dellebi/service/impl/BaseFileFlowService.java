package com.latticeengines.dellebi.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StreamUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dellebi.service.FileFlowService;
import com.latticeengines.dellebi.service.FileType;

public abstract class BaseFileFlowService implements FileFlowService {

    static final Log log = LogFactory.getLog(BaseFileFlowService.class);

    static final int FAIL_TRIES = 3;
    private Map<String, Integer> failedFiles = new HashMap<>();

    @Value("${dellebi.quoteheaders}")
    String quoteHeaders;
    
    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;
    
    @Value("${dellebi.datahadooprootpath}")
    private String datahadooprootpath;
    
    @Value("${dellebi.datahadooperrorworkingpath}")
    private String dataHadoopErrorWorkingPath;

    public BaseFileFlowService() {
        super();
    }

    @Override
    public FileType getFileType(String zipFileName) {
        if (zipFileName.startsWith("tgt_quote_trans_global")) {
            return FileType.QUOTE;
        }
    
        if (zipFileName.startsWith("tgt_lat_order_summary_global")) {
            return FileType.ORDER_SUMMARY;
        }
    
        if (zipFileName.startsWith("tgt_order_detail_global")) {
            return FileType.ORDER_DETAIL;
        }
    
        if (zipFileName.startsWith("tgt_ship_to_addr_lattice")) {
            return FileType.SHIP;
        }
    
        if (zipFileName.startsWith("tgt_warranty_global")) {
            return FileType.WARRANTE;
        }
        return null;
    }

    protected boolean isFailedFile(String zipFileName) {
        zipFileName = zipFileName.toLowerCase();
        Integer count = failedFiles.get(zipFileName);
        if (count == null) {
            return false;
        }
        return count > FAIL_TRIES;
    }

    @Override
    public void registerFailedFile(String fileName) {
        fileName = fileName.toLowerCase();
        if (fileName.endsWith(".txt")) {
            fileName = StringUtils.removeEnd(fileName, ".txt") + ".zip";
        }
        Integer count = failedFiles.get(fileName);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        failedFiles.put(fileName, count);
        if (count > FAIL_TRIES) {
            log.info("Found bad file name=" + fileName);
        }
    }

    protected String downloadAndUnzip(InputStream is, String fileName) {
        String zipDir = getZipDir();
        String txtDir = getTxtDir();
    
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            if (!HdfsUtils.fileExists(conf, zipDir)) {
                HdfsUtils.mkdir(conf, zipDir);
            }
            String zipFilePath = zipDir + "/" + fileName;
            OutputStream os = fs.create(new Path(zipFilePath), true);
            log.info("Starting to download file to HDFS, fileName=" + fileName);
            FileCopyUtils.copy(is, os);
            log.info("Finished downloading file to HDFS, fileName=" + fileName);
    
            if (HdfsUtils.fileExists(conf, txtDir)) {
                HdfsUtils.rmdir(conf, txtDir);
            }
            return unzip(fs, zipDir, txtDir, fileName);
    
        } catch (Exception ex) {
            log.error("Can not download or unzip File, name=" + fileName, ex);
            return null;
        }
    }

    private String unzip(FileSystem fs, String zipDir, String txtDir, String zipFileName) throws Exception {
        int idx = zipFileName.lastIndexOf(".");
        if (idx < 0) {
            log.info("It's not a zip file!");
            return null;
        }
        String ext = zipFileName.substring(idx + 1);
        if (!"zip".equalsIgnoreCase(ext)) {
            log.info("It's not a zip file!");
            return null;
        }
    
        String inputFile = zipDir + "/" + zipFileName;
        ZipInputStream zipIn = new ZipInputStream(fs.open((new Path(inputFile))));
        ZipEntry entry = zipIn.getNextEntry();
        String txtFileName = null;
        while (entry != null) {
            txtFileName = entry.getName();
            String txtFilePath = txtDir + "/" + txtFileName;
            if (!entry.isDirectory()) {
                FSDataOutputStream os = fs.create(new Path(txtFilePath));
                try {
                    if (zipFileName.startsWith("tgt_quote_trans_global")
                            && !zipFileName.startsWith("tgt_quote_trans_global_1_")) {
                        StreamUtils.copy((quoteHeaders + "\n").getBytes(), os);
                    }
                    StreamUtils.copy(zipIn, os);
                } finally {
                    if (os != null) {
                        os.close();
                    }
                    zipIn.closeEntry();
                }
            }
            entry = zipIn.getNextEntry();
    
        }
        zipIn.close();
        return txtFileName;
    }
    
    @Override
    public String getTxtDir() {
        return getQualifiedHadoopPath() + "/txt_dir";
    }

    @Override
    public String getZipDir() {
        return getQualifiedHadoopPath() + "/zip_dir";
    }

    @Override
    public String getOutputDir() {
        return getQualifiedHadoopPath() + "/output";
    }
    
    @Override
    public String getErrorOutputDir() {
        return datahadooprootpath + '/' + dataHadoopErrorWorkingPath;
    }
    
    private String getQualifiedHadoopPath() {
        return datahadooprootpath + '/' + dataHadoopWorkingPath;
    }
}