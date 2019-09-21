package com.latticeengines.dellebi.dataprocess;

import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StreamUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

@Deprecated
@ContextConfiguration(locations = { "classpath:common-properties-context.xml", })
public class BatchDownloadTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(BatchDownloadTestNG.class);

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.smbaccount}")
    private String smbAccount;
    @Value("${dellebi.smbps}")
    private String smbPS;

    @Value("${dellebi.smbarchivepath}")
    private String smbArchivePath;

    @Autowired
    private Configuration yarnConfiguration;

    @Test(groups = "manual", enabled = false)
    public void batchDownload() throws Exception {

        String batchInputDir = dataHadoopWorkingPath + "/ZipDir/";
        String batchOutputDir = dataHadoopWorkingPath + "/TextDir/";
        if (HdfsUtils.fileExists(yarnConfiguration, batchInputDir)) {
            HdfsUtils.rmdir(yarnConfiguration, batchInputDir);
        }
        if (HdfsUtils.fileExists(yarnConfiguration, batchOutputDir)) {
            HdfsUtils.rmdir(yarnConfiguration, batchOutputDir);
        }

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        SmbFile smbDir = new SmbFile(smbArchivePath + "/", auth);

        SmbFile[] files = smbDir.listFiles();

        for (SmbFile file : files) {
            long startTime = System.currentTimeMillis();
            System.out.println("Downloading zip file name=" + file.getName());

            FileSystem fs = FileSystem.get(yarnConfiguration);
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
