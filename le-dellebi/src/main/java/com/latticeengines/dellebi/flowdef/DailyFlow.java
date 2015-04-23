package com.latticeengines.dellebi.flowdef;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import com.latticeengines.dellebi.util.FileSystemOperations;
import com.latticeengines.dellebi.util.HadoopFileSystemOperations;
import com.latticeengines.dellebi.util.NormalFileSystemOperations;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.property.AppProps;

public class DailyFlow {

    private final static Logger LOGGER = Logger.getLogger(DailyFlow.class);
    @Value("${dellebi.datahadoopinpath}")
    private String dataHadoopInPath;

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
    
    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    private ArrayList<FlowDef> flowList;

    public DailyFlow(ArrayList<FlowDef> flowList) {
        this.flowList = flowList;
    }

    public void doDailyFlow(ApplicationContext springContent) {
        
        LOGGER.info("All daily refresh files arrive!");
        LOGGER.info("Start process data on HDFS!");
        

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, DailyFlow.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        LOGGER.info("Start to process income files.");
        
        FileSystemOperations fileSystem = null;

        if(System.getProperty("DELLEBI_PROPDIR").contains("conf/env/local")){
            fileSystem = springContent.getBean("normaloperation", NormalFileSystemOperations.class);
        }else{
            fileSystem = springContent.getBean("hadoopoperation", HadoopFileSystemOperations.class);
        }
   

        try {
            for (Iterator<FlowDef> flow = flowList.iterator(); flow.hasNext();) {
                FlowDef item = flow.next();
                
                if ("OrderSumDailyFlow".equals(item.getName()) && (fileSystem.listFileNumber(dataHadoopInPath + "/" + orderSummary) != 0)) {
                    flowConnector.connect(item).complete();
                    //Remove .txt file after cascading processes data.
                    fileSystem.cleanFolder(dataHadoopInPath + "/" + orderSummary);
                } else if ("OrderDetailDailyFlow".equals(item.getName()) && (fileSystem.listFileNumber(dataHadoopInPath + "/" + orderDetail) != 0)) {
                    flowConnector.connect(item).complete();
                    fileSystem.cleanFolder(dataHadoopInPath + "/" + orderDetail);
                } else if ("ShipDailyFlow".equals(item.getName()) && (fileSystem.listFileNumber(dataHadoopInPath + "/" + shipToAddrLattice) != 0)) {
                    flowConnector.connect(item).complete();
                    fileSystem.cleanFolder(dataHadoopInPath + "/" + shipToAddrLattice);
                } else if ("WarrantyDailyFlow".equals(item.getName()) && (fileSystem.listFileNumber(dataHadoopInPath + "/" + warrantyGlobal) != 0)) {
                    flowConnector.connect(item).complete();
                    fileSystem.cleanFolder(dataHadoopInPath + "/" + warrantyGlobal);
                } else if ("QuoteTransDailyFlow".equals(item.getName()) && (fileSystem.listFileNumber(dataHadoopInPath + "/" + quoteTrans) != 0)) {
                    flowConnector.connect(item).complete();
                    fileSystem.cleanFolder(dataHadoopInPath + "/" + quoteTrans);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed!", e);
        }

    }
    
    public void setDataHadoopInPath(String s){
        this.dataHadoopInPath = s;
    }
    
    public void setOrderSummary(String s){
        this.orderSummary = s;
    }
    
    public void setOrderDetail(String s){
        this.orderDetail = s;
    }
    
    public void setShipToAddrLattice(String s){
        this.shipToAddrLattice = s;
    }
    
    public void setWarrantyGlobal(String s){
        this.warrantyGlobal = s;
    }
    
    public void setDataHadoopWorkingPath(String s){
        this.dataHadoopWorkingPath = s;
    }
}
