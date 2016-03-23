package com.latticeengines.dellebi.dataprocess;

import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StreamUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;

@ContextConfiguration(locations = { "classpath:dellebi-properties-context.xml", })
public class BatchDownloadTestNG extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(BatchDownloadTestNG.class);

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.smbaccount}")
    private String smbAccount;
    @Value("${dellebi.smbps}")
    private String smbPS;

    @Value("${dellebi.smbarchivepath}")
    private String smbArchivePath;

    @Test(groups = "manual")
    public void batchDownload() throws Exception {

        Configuration conf = null;
        conf = new Configuration();

        String batchInputDir = dataHadoopWorkingPath + "/ZipDir/";
        String batchOutputDir = dataHadoopWorkingPath + "/TextDir/";
        if (HdfsUtils.fileExists(conf, batchInputDir)) {
            HdfsUtils.rmdir(conf, batchInputDir);
        }
        if (HdfsUtils.fileExists(conf, batchOutputDir)) {
            HdfsUtils.rmdir(conf, batchOutputDir);
        }

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        SmbFile smbDir = new SmbFile(smbArchivePath + "/", auth);

        SmbFile[] files = smbDir.listFiles();

        for (SmbFile file : files) {
            long startTime = System.currentTimeMillis();
            System.out.println("Downloading zip file name=" + file.getName());

            FileSystem fs = FileSystem.get(conf);
            String filePath = batchInputDir + file.getName();
            OutputStream os = fs.create(new Path(filePath));
            FileCopyUtils.copy(file.getInputStream(), os);

            long endTime = System.currentTimeMillis();
            System.out.println("Downloaded zip Time elapsed="
                    + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));

            unzip(fs, batchInputDir, batchOutputDir, file.getName());

            endTime = System.currentTimeMillis();
            System.out.println(
                    "Unzip Time elapsed=" + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));

        }

    }

    private void unzip(FileSystem fs, String batchInputDir, String batchOutputDir, String name) {
        int idx = name.lastIndexOf(".");
        if (idx < 0) {
            log.info("It's not a zip file!");
            return;
        }
        String ext = name.substring(idx + 1);
        if (!"zip".equalsIgnoreCase(ext)) {
            log.info("It's not a zip file!");
            return;
        }

        String inputFile = batchInputDir + name;
        unzipOnHdfs(fs, inputFile, batchOutputDir);

    }

    private void unzipOnHdfs(FileSystem fs, String inputFile, String batchOutputDir) {

        try (ZipInputStream zipIn = new ZipInputStream(fs.open((new Path(inputFile))))) {
            ZipEntry entry = zipIn.getNextEntry();
            while (entry != null) {
                String outputFilePath = batchOutputDir + entry.getName();
                if (!entry.isDirectory()) {
                    StreamUtils.copy(zipIn, fs.create(new Path(outputFilePath)));
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry();
            }
        } catch (Exception ex) {
            log.error("Failed to unzip file=" + inputFile, ex);
        }
    }

}
