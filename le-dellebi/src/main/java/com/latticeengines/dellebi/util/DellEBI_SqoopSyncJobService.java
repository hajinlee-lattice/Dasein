package com.latticeengines.dellebi.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.sqoop.LedpSqoop;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;




public class DellEBI_SqoopSyncJobService {

	
    private Configuration yarnConfiguration;
	
	protected static final int MAX_TRIES = 60;
	protected static final long APP_WAIT_TIME = 1000L;
	
	private static final Log log = LogFactory.getLog(DellEBI_SqoopSyncJobService.class);

    public int exportData(String table, String sourceDir, String uri, String queue, String customer,
            int numMappers, String javaColumnTypeMappings) {

        final String jobName = customer;
        int rc = 0;
        rc = exportSync(table, sourceDir, uri, queue, jobName, numMappers, javaColumnTypeMappings);

        return rc;
    }
    
    private int exportSync(final String table, final String sourceDir, final String uri, final String queue,
            final String jobName, final int numMappers, String javaColumnTypeMappings) {
        List<String> cmds = new ArrayList<>();
        int rc = 1;
        cmds.add("export");
        cmds.add("-Dhadoop.mapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(uri);
        cmds.add("--m");
        cmds.add(Integer.toString(numMappers));
        cmds.add("--table");
        cmds.add(table);
 //       cmds.add("--mapreduce-job-name");
 //       cmds.add(jobName);
        cmds.add("--export-dir");
        cmds.add(sourceDir);
        if (javaColumnTypeMappings != null) {
            cmds.add("--columns");
            cmds.add(javaColumnTypeMappings);
        }
        rc = LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration());
        return rc;
    }

	
}
