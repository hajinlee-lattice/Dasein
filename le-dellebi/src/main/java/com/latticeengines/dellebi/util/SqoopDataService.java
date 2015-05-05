package com.latticeengines.dellebi.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

public class SqoopDataService {
	
	private static final Log log = LogFactory.getLog(SqoopDataService.class);
	
	@Autowired
    private SqoopSyncJobService sqoopSyncJobService;
	
    @Autowired
    private DbCreds scoringCreds_2;
    
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
	
    
    @Value("${dellebi.customer}")
    private String customer;
  
    @Autowired
    private MetadataService metadataService;
    
    @Autowired
    private HadoopFileSystemOperations hadoopfilesystemoperations;
    
      
    public ApplicationId export() {
    	
    	ApplicationId appId = null;
    	
    	String targetTable = targetRawTable;

    	String sourceDir = dataHadoopWorkingPath + "/" + quotetrans;
       	String columns = "\"QuoteNumber,Date,CustomerID,Product,RepBadge,Quantity,Amount,QuoteFileName\"";
       	log.info("Start export HDFS files " + sourceDir);
    	String queue = LedpQueueAssigner.getMRQueueNameForSubmission();

    	log.info("SQL Server URI: "+metadataService.getJdbcConnectionUrl(scoringCreds_2));
    	appId = sqoopSyncJobService.exportData(targetTable, sourceDir, scoringCreds_2, queue, customer, 4, columns);
   	  
    	log.info("Remove the exported HDFS file" + sourceDir);
    	hadoopfilesystemoperations.cleanFolder(sourceDir);
    	    	
    	log.info("Finish export HDFS files to SQL server");
    	
        return appId;
    	    
    }
    
}
