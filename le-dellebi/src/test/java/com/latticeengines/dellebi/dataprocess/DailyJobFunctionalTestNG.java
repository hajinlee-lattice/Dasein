package com.latticeengines.dellebi.dataprocess;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.util.FileSystemOperations;

@ContextConfiguration(locations = { "classpath:dellebi-properties-context.xml" })
public class DailyJobFunctionalTestNG extends AbstractTestNGSpringContextTests {

    @Value("${dellebi.cameldataincomepath}")
    private String camelDataIncomePath;
    @Value("${dellebi.cameldataarchivepath}")
    private String camelDataArchivepath;

    @Value("${dellebi.ordersummary}")
    private String orderSummary;
    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;
    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.smbaccount}")
    private String smbAccount;
    @Value("${dellebi.smbps}")
    private String smbPS;
    @Value("${dellebi.smbinboxpath}")
    private String smbInboxPath;
    @Value("${dellebi.smbarchivepath}")
    private String smbArchivePath;

    @BeforeMethod(groups = "functional")
    public void setUpBeforeMethod() throws Exception {
        ApplicationContext springContext = new ClassPathXmlApplicationContext("test-dellebi-properties-context.xml");
        
        smbPut("smb://192.168.4.145/DATASTORE/Dataload/TestInbox", "./src/test/resources/tgt_lat_order_summary_global_2014.zip");  

        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
        }
        
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            if (fs.exists(new Path(dataHadoopWorkingPath + "/" + orderSummary))) {
                fs.delete(new Path(dataHadoopWorkingPath + "/" + orderSummary), true);                                                                            // Directory
            }
        } catch (Exception e) {
            System.out.println("HDFS file not found");
        }

    }

    @AfterMethod(groups = "functional")
    public void setUpAfterMethod() throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
        
        SmbFile smbFile = new SmbFile(smbArchivePath + "/tgt_lat_order_summary_global_2014.zip", auth);
        if (smbFile.canWrite()) {
            smbFile.delete();
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
        Path path = new Path(dataHadoopWorkingPath + "/" + orderSummary);
        if (fs.exists(path)) {
            fs.delete(path, true); 
        }
    }

    @Test(groups = "functional")
    public void testExecute() throws Exception {
        ApplicationContext springContext = new ClassPathXmlApplicationContext("dellebi-properties-context.xml",
                "dellebi-context.xml");

        // Wait for a while to let Camel process data.
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
        }

        // Process data using Cascading
        DailyFlow dailFlow = springContext.getBean("dailyFlow", DailyFlow.class);
        dailFlow.doDailyFlow(springContext);
        
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
        Path path = new Path(dataHadoopWorkingPath + "/" + orderSummary);
        Assert.assertEquals(fs.exists(new Path(dataHadoopWorkingPath + "/" + orderSummary)), true);
    }
    
    public void smbPut(String remoteUrl,String localFilePath){
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
        
        InputStream in = null;  
        OutputStream out = null;  
        try {  
            File localFile = new File(localFilePath);  
            String fileName = localFile.getName();  
            SmbFile remoteFile = new SmbFile(remoteUrl+"/"+fileName, auth);  
            in = new BufferedInputStream(new FileInputStream(localFile));  
            out = new BufferedOutputStream(new SmbFileOutputStream(remoteFile));  
            byte []buffer = new byte[1024];  
            while((in.read(buffer)) != -1){  
                out.write(buffer);  
                buffer = new byte[1024];  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        }finally{  
            try {  
                out.close();  
                in.close();  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
    }  
}
