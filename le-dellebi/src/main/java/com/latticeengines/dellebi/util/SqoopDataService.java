package com.latticeengines.dellebi.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dellebi.util.DellEBI_SqoopSyncJobService;


public class SqoopDataService {
	
	private static final Log log = LogFactory.getLog(SqoopDataService.class);
	
	private DellEBI_SqoopSyncJobService sqoopSyncJobService;
	    
    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;
    
    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;
    
    @Value("${dellebi.ordersummary}")
    private String orderSummary;
    
    @Value("${dellebi.quotetrans}")
    private String quotetrans;
	
    @Value("${dellebi.output.table.sample}")
    private String targetRawTable;
	
    @Value("${dellebi.sqlserverconnection}")
    private String uri;
    
    @Value("${dellebi.customer}")
    private String customer;
    
    @Value("${dellebi.quotetrans.storeprocedure")
    private String quote_sp;
      
    @Autowired
    private HadoopFileSystemOperations hadoopfilesystemoperations;
    
      
    public void export() {
    	    	
    	String targetTable = targetRawTable;
    	String queue = null;
    	int rc = 1;
    	String sourceDir = dataHadoopRootPath + dataHadoopWorkingPath + "/" + quotetrans;
       	String columns = "QuoteNumber,Date,CustomerID,Product,RepBadge,Quantity,Amount,QuoteFileName";
       	String sqlStr = "exec "+quote_sp;
       	log.info("Start export HDFS files " + sourceDir);

       	sqoopSyncJobService = new DellEBI_SqoopSyncJobService();
 
       	log.info("SQL Server URI: "+ uri );
    	
       	try {
       		rc = sqoopSyncJobService.exportData(targetTable, sourceDir, uri, queue, customer, 4, columns);
       	}
       	catch (Exception e)
       	{
       		log.error("Exception: Export files "+ sourceDir +" to SQL server failed");
       	}
       	
       	if (rc == 0){
       		log.info("Begin to execute the Store Procedure: " + quote_sp);
       		rc = sqoopSyncJobService.eval(sqlStr, 1, uri);
       	}
       	
       	if (rc == 0){
       		log.info("Remove the exported HDFS file" + sourceDir);
        	hadoopfilesystemoperations.cleanFolder(sourceDir);
       	}  
       	else {
       		log.error("Export files "+ sourceDir +" to SQL server failed");
       	}
    	  	
    	log.info("Finish export HDFS files to SQL server");
    	
        return ;
    	    
    }
    
}
