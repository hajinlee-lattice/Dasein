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
import com.latticeengines.dellebi.util.SqoopDataService;

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
		ApplicationContext springContext = new ClassPathXmlApplicationContext(
				"dellebi-properties-context.xml");

		// Copy test files to remote test folder.
		smbPut(smbInboxPath,
				"./src/test/resources/tgt_quote_trans_global_1_2015.zip");
		smbPut(smbInboxPath,
				"./src/test/resources/tgt_quote_trans_global_4_2015.zip");

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
		}

		try {
			// Remove output folder on HDFS.
			Path quotePath = new Path(dataHadoopWorkingPath + "/" + quoteTrans);
			deleteHDFSFolder(quotePath);

		} catch (Exception e) {
			System.out.println("HDFS file not found");
		}

	}

	@AfterMethod(groups = "functional")
	public void setUpAfterMethod() throws Exception {

		NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("",
				smbAccount, smbPS);

		deleteSMBFile(new SmbFile(smbArchivePath
				+ "/tgt_quote_trans_global_1_2015.zip", auth));
		deleteSMBFile(new SmbFile(smbArchivePath
				+ "/tgt_quote_trans_global_4_2015.zip", auth));

		// Remove output folder on HDFS.
		Path quotePath = new Path(dataHadoopWorkingPath + "/" + quoteTrans);
		//deleteHDFSFolder(quotePath);
	}

	@Test(groups = "functional")
	public void testExecute() throws Exception {
		ApplicationContext springContext = new ClassPathXmlApplicationContext(
				"dellebi-properties-context.xml", "dellebi-camel-context.xml",
				"dellebi-component-context.xml");

		// Wait for a while to let Camel process data.
		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
		}

		// Process data using Cascading
		DailyFlow dailyFlow = springContext.getBean("dailyFlow",
				DailyFlow.class);
		dailyFlow.setDataHadoopInPath("/latticeengines/in");
		dailyFlow.setDataHadoopWorkingPath("/latticeengines");
		dailyFlow.setOrderDetail("order_detail");
		dailyFlow.setOrderSummary("order_summary");
		dailyFlow.setShipToAddrLattice("ship_to_addr_lattice");
		dailyFlow.setWarrantyGlobal("warranty_global");
		dailyFlow.setQuoteTrans("quote_trans");
		dailyFlow.setMailReceiveList("llu@Lattice-Engines.com,jwilliams@lattice-engines.com,LYan@Lattice-Engines.com");
		dailyFlow.doDailyFlow();

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
		Path quoteTransPath = new Path(dataHadoopWorkingPath + "/" + quoteTrans);

		Assert.assertEquals(fs.exists(quoteTransPath), true);

		SqoopDataService sqoopDataService = springContext.getBean(
				"sqoopDataService", SqoopDataService.class);

		sqoopDataService.export();

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
		}

//		Assert.assertEquals(fs.exists(quoteTransPath), false);
	}

	private void smbPut(String remoteUrl, String localFilePath) {
		NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("",
				smbAccount, smbPS);

		InputStream in = null;
		OutputStream out = null;
		try {
			File localFile = new File(localFilePath);
			String fileName = localFile.getName();
			SmbFile remoteFile = new SmbFile(remoteUrl + "/" + fileName, auth);
			in = new BufferedInputStream(new FileInputStream(localFile));
			out = new BufferedOutputStream(new SmbFileOutputStream(remoteFile));
			byte[] buffer = new byte[1024];
			while ((in.read(buffer)) != -1) {
				out.write(buffer);
				buffer = new byte[1024];
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void deleteHDFSFolder(Path path) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	private void deleteSMBFile(SmbFile smbFile) throws Exception {

		if (smbFile.canWrite()) {
			smbFile.delete();
		}
	}
}
