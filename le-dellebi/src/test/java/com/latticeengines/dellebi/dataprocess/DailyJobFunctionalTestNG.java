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
	@Value("${dellebi.orderdetail}")
    private String orderDetail;
	@Value("${dellebi.shiptoaddrlattice}")
    private String shipToAddrLattice;
	@Value("${dellebi.warrantyglobal}")
    private String warrantyGlobal;
	@Value("${dellebi.quotetrans}")
    private String quoteTrans;
	
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
		smbPut("smb://192.168.4.145/DATASTORE/Dataload/TestInbox", "./src/test/resources/tgt_order_detail_global_2_20141214_201155.zip");
		smbPut("smb://192.168.4.145/DATASTORE/Dataload/TestInbox", "./src/test/resources/tgt_ship_to_addr_lattice_1_20141109_184552.zip");
		smbPut("smb://192.168.4.145/DATASTORE/Dataload/TestInbox", "./src/test/resources/tgt_warranty_global_1_20141213_190323.zip");
		smbPut("smb://192.168.4.145/DATASTORE/Dataload/TestInbox", "./src/test/resources/tgt_quote_trans_global_1_2015.zip");

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
        
        SmbFile smbOrderSummaryFile = new SmbFile(smbArchivePath + "/tgt_lat_order_summary_global_2014.zip", auth);
        if (smbOrderSummaryFile.canWrite()) {
            smbOrderSummaryFile.delete();
        }
		SmbFile smbOrderDetailFile = new SmbFile(smbArchivePath + "/tgt_order_detail_global_2_20141214_201155.zip", auth);
        if (smbOrderDetailFile.canWrite()) {
            smbOrderDetailFile.delete();
        }
		SmbFile smbShipFile = new SmbFile(smbArchivePath + "/tgt_ship_to_addr_lattice_1_20141109_184552.zip", auth);
        if (smbShipFile.canWrite()) {
            smbShipFile.delete();
        }
		SmbFile smbWarrantyFile = new SmbFile(smbArchivePath + "/tgt_warranty_global_1_20141213_190323.zip", auth);
        if (smbWarrantyFile.canWrite()) {
            smbWarrantyFile.delete();
        }
		SmbFile smbQuoteFile = new SmbFile(smbArchivePath + "/tgt_quote_trans_global_1_2015.zip", auth);
        if (smbQuoteFile.canWrite()) {
            smbQuoteFile.delete();
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
        Path orderSummaryPath = new Path(dataHadoopWorkingPath + "/" + orderSummary);
		Path orderDetailPath = new Path(dataHadoopWorkingPath + "/" + orderDetail);
		Path shipToAddrLatticePath = new Path(dataHadoopWorkingPath + "/" + shipToAddrLattice);
		Path warrantyGlobalPath = new Path(dataHadoopWorkingPath + "/" + warrantyGlobal);
		Path quoteTransPath = new Path(dataHadoopWorkingPath + "/" + quoteTrans);
			
        Assert.assertEquals(fs.exists(orderSummaryPath), true);
		Assert.assertEquals(fs.exists(orderDetailPath), true);
		Assert.assertEquals(fs.exists(shipToAddrLatticePath), true);
		Assert.assertEquals(fs.exists(warrantyGlobalPath), true);
		Assert.assertEquals(fs.exists(quoteTransPath), true);
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
