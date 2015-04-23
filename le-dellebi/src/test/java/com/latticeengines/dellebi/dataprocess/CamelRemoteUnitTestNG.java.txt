package com.latticeengines.dellebi.dataprocess;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.zipfile.ZipSplitter;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.net.ftp.FTPClient;
import org.testng.annotations.Test;

public class CamelRemoteUnitTestNG {

    @Test(groups = "unit")
    public void copyFiles() throws Exception {

        smbPut("smb://192.168.4.145/DATASTORE/Dataload/TestInbox", "/home/lillian/settings.xml"); 
    }
   
    //@Test(groups = "unit")
    public void copyFilesToHDFS() throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("smb://LATTICE;llu@192.168.4.145/datastore/dataload/testinbox?password=Blue15Garden")
                .split(new ZipSplitter()).streaming().to("mock:processZipEntry")
                .to("hdfs2://localhost:9000/user/order_summary");
            }
        });
        context.start();
        Thread.sleep(10000);
        context.stop();
    }
    
public void smbPut(String remoteUrl,String localFilePath){  
        
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", "LATTICE\\llu", "Blue15Garden");
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
