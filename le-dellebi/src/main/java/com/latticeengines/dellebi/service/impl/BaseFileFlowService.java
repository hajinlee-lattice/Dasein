package com.latticeengines.dellebi.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StreamUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.dellebi.service.FileFlowService;
import com.latticeengines.dellebi.service.FileType;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public abstract class BaseFileFlowService implements FileFlowService {

    static final Log log = LogFactory.getLog(BaseFileFlowService.class);

    static final int FAIL_TRIES = 3;
    private Map<String, Integer> failedFiles = new HashMap<>();

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.datahadooperrorworkingpath}")
    private String dataHadoopErrorWorkingPath;

    @Autowired
    protected DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    @Autowired
    protected DellEbiExecutionLogEntityMgr dellEbiExecutionLogEntityMgr;

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
            return FileType.WARRANTY;
        }

        if (zipFileName.startsWith("tgt_lattice_mfg_ext")) {
            return FileType.SKU_MANUFACTURER;
        }

        if (zipFileName.startsWith("tgt_itm_cls_code")) {
            return FileType.SKU_ITM_CLS_CODE;
        }

        if (zipFileName.startsWith("tgt_all_chnl")) {
            return FileType.CHANNEL;
        }

        if (zipFileName.startsWith("global_sku_lookup")) {
            return FileType.SKU_GLOBAL;
        }

        if (zipFileName.startsWith("fiscal_day_calendar")) {
            return FileType.CALENDAR;
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
        String txtDir = getTxtDir(getFileType(fileName).getType());

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
            throw new LedpException(LedpCode.LEDP_29004, ex, new String[] { fileName });
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

        FileType type = getFileType(zipFileName);
        String typeName = type.toString();
        String headers = dellEbiConfigEntityMgr.getHeaders(typeName);
        while (entry != null) {
            txtFileName = entry.getName();
            String txtFilePath = txtDir + "/" + txtFileName;
            if (!entry.isDirectory()) {
                FSDataOutputStream os = fs.create(new Path(txtFilePath));
                try {
                    if (isNeededAddHeader(zipFileName)) {
                        StreamUtils.copy((headers + "\n").getBytes(), os);
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

    private Boolean isNeededAddHeader(String fileName) {

        if (!fileName.contains("_1_"))
            return true;
        if (fileName.contains("global_sku_lookup"))
            return true;

        return false;
    }

    @Override
    public String getTxtDir(String type) {
        return dataHadoopWorkingPath + "/" + type + "/txt_dir";
    }

    @Override
    public String getZipDir() {
        return dataHadoopWorkingPath + "/zip_dir";
    }

    @Override
    public String getOutputDir(String type) {
        return dataHadoopWorkingPath + "/" + type + "/output";
    }

    @Override
    public String getErrorOutputDir() {
        return dataHadoopErrorWorkingPath;
    }

    protected boolean isActive(String fileName) {

        FileType type = getFileType(fileName);
        if (type == null)
            return false;
        String typeName = type.getType();

        Boolean isActive = dellEbiConfigEntityMgr.getIsActive(typeName);

        if (isActive == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        return isActive;
    }

    protected boolean isProcessedFile(String fileName) {

        DellEbiExecutionLog dellEbiExecutionLog = dellEbiExecutionLogEntityMgr.getEntryByFile(fileName);

        if (dellEbiExecutionLog == null) {
            return false;
        }

        if (dellEbiExecutionLog.getStatus() == DellEbiExecutionLogStatus.Completed.getStatus()) {
            return true;
        }

        return false;
    }

    protected boolean isValidForDate(String fileName, Long lastModified) {
        Long dateLong;
        FileType type = getFileType(fileName);
        if (type == null)
            return false;

        String typeName = type.getType();

        Date startDate = dellEbiConfigEntityMgr.getStartDate(typeName);

        if (startDate == null) {
            dateLong = -1L;
        } else {
            dateLong = startDate.getTime();
        }

        if (lastModified > dateLong) {
            return true;
        }

        return false;

    }

}