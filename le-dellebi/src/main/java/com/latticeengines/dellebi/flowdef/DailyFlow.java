package com.latticeengines.dellebi.flowdef;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.planner.PlannerException;
import cascading.property.AppProps;

import com.latticeengines.dellebi.util.FileSystemOperations;
import com.latticeengines.dellebi.util.HadoopFileSystemOperations;
import com.latticeengines.dellebi.util.MailSender;
import com.latticeengines.dellebi.util.NormalFileSystemOperations;

//@Component("dailyFlow")
public class DailyFlow{

    private static final Log log = LogFactory.getLog(DailyFlow.class);
    
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
    
    @Value("${dellebi.mailreceivelist}")
    private String mailReceiveList;
    
    private ArrayList<FlowDef> flowList;
    
    public DailyFlow(ArrayList<FlowDef> flowList){
    	this.flowList = flowList;
    }

    public void doDailyFlow() {  	
        
    	log.info("All daily refresh files arrive!");
    	log.info("Start process data on HDFS!");
        
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, DailyFlow.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        log.info("Start to process income files.");
        
        FileSystemOperations fileSystem = null;

        if(System.getProperty("DELLEBI_PROPDIR").contains("conf/env/local")){        	
            fileSystem = new NormalFileSystemOperations();
        }else{
            fileSystem = new HadoopFileSystemOperations();
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
        } catch(PlannerException e){
        	log.error("Seems there is corrupt data!", e);
        } catch (Exception e) {
        	log.warn("Failed!", e);
            //mailSender.sendEmail(mailReceiveList,"Dell EBI daily refresh just failed for some reasons!","Please check Dell EBI logs.");
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
