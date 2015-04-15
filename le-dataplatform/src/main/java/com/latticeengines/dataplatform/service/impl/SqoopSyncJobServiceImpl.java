package com.latticeengines.dataplatform.service.impl;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.sqoop.LedpSqoop;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.dataplatform.runtime.load.LoadProperty;
import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;

@Component("sqoopSyncJobService")
public class SqoopSyncJobServiceImpl extends JobServiceImpl implements SqoopSyncJobService {

    @Autowired
    private JobNameService jobNameService;

    @Autowired
    private MetadataService metadataService;

    @Override
    public ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer, List<String> splitCols, Map<String, String> properties) {
        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        return importData(table, targetDir, creds, queue, customer, splitCols, properties, numDefaultMappers);
    }

    @Override
    public ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer, List<String> splitCols, Map<String, String> properties,
            int numMappers) {

        final String jobName = jobNameService.createJobName(customer, "sqoop-import");

        importSync(table, targetDir, creds, queue, jobName, splitCols, properties, numMappers);

        return getApplicationId(jobName);
    }

    private ApplicationId getApplicationId(final String jobName) {
        int tries = 0;
        ApplicationId appId = null;
        while (tries < MAX_TRIES) {
            try {
                Thread.sleep(APP_WAIT_TIME);
            } catch (InterruptedException e) {
                log.warn("Thread.sleep interrupted.", e);
            }
            appId = getAppIdFromName(jobName);
            if (appId != null) {
                return appId;
            }
            tries++;
        }
        return appId;
    }

    private void importSync(final String table, final String targetDir, final DbCreds creds, final String queue, final String jobName,
            final List<String> splitCols, final Map<String, String> properties, final int numMappers) {
        List<String> cmds = new ArrayList<>();
        cmds.add("import");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(metadataService.getJdbcConnectionUrl(creds));
        cmds.add("--m");
        cmds.add(Integer.toString(numMappers));
        cmds.add("--table");
        cmds.add(table);
        cmds.add("--as-avrodatafile");
        cmds.add("--compress");
        cmds.add("--mapreduce-job-name");
        cmds.add(jobName);
        if(!properties.isEmpty()){
            cmds.add("--columns");
            cmds.add(generateColumnList(table, creds, properties));
        }
        cmds.add("--split-by");
        cmds.add(StringUtils.join(splitCols, ","));
        cmds.add("--target-dir");
        cmds.add(targetDir);
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(yarnConfiguration));

    }

    private String generateColumnList(String table, DbCreds creds, Map<String, String> properties){
        StringBuilder lb = new StringBuilder();
        try {
            DataSchema dataSchema = metadataService.createDataSchema(creds, table);
            List<Field> fields = dataSchema.getFields();

            boolean excludeTimestampCols = Boolean.parseBoolean(LoadProperty.EXCLUDETIMESTAMPCOLUMNS.getValue(properties));
            boolean first = true;
            for (Field field : fields) {
                
                // The scoring engine does not know how to convert datetime columns into a numeric value, 
                // which Sqoop does automatically. This should not be a problem now since dates are
                // typically not predictive anyway so we can safely exclude them for now.
                // We can start including TIMESTAMP and TIME columns by explicitly setting EXCLUDETIMESTAMPCOLUMNS=false
                // in the load configuration.
                if (excludeTimestampCols && (field.getSqlType() == Types.TIMESTAMP || field.getSqlType() == Types.TIME)) {
                    continue;
                }
                String name = field.getName();
                String colName = field.getColumnName();
                
                if (name == null) {
                    log.warn("Field name is null.");
                    continue;
                }
                if (colName == null) {
                    log.warn("Column name is null.");
                    continue;
                }
                if (!first) {
                    lb.append(",");
                } else {
                    first = false;
                }
                lb.append(colName);
                if (!colName.equals(name)) {
                    log.warn(LedpException.buildMessageWithCode(LedpCode.LEDP_11005, new String[] { colName, name }));
                }
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_11004, new String[] { table });
        }
        return lb.toString();
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return null;
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, String queue, String customer, String jdbcUrl) {

        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        return exportData(table, sourceDir, queue, customer, numDefaultMappers, jdbcUrl);
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, String queue, String customer, int numMappers,
            String jdbcUrl) {

        final String jobName = jobNameService.createJobName(customer, "sqoop-export");

        exportSync(table, sourceDir, queue, jobName, numMappers, jdbcUrl, null);

        return getApplicationId(jobName);
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, String queue, String customer, int numMappers,
            String jdbcUrl, String javaColumnTypeMappings) {

        final String jobName = jobNameService.createJobName(customer, "sqoop-export");

        exportSync(table, sourceDir, queue, jobName, numMappers, jdbcUrl, javaColumnTypeMappings);

        return getApplicationId(jobName);
    }

    private void exportSync(final String table, final String sourceDir, final String queue, final String jobName,
            final int numMappers, String jdbcUrl, String javaColumnTypeMappings) {
        List<String> cmds = new ArrayList<>();
        cmds.add("export");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(jdbcUrl);
        cmds.add("--m");
        cmds.add(Integer.toString(numMappers));
        cmds.add("--table");
        cmds.add(table);
        cmds.add("--direct");
        cmds.add("--mapreduce-job-name");
        cmds.add(jobName);
        cmds.add("--export-dir");
        cmds.add(sourceDir);
        if (javaColumnTypeMappings != null) {
            cmds.add("--map-column-java");
            cmds.add(javaColumnTypeMappings);
        }
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(yarnConfiguration));
    }

    @Override
    public void eval(String sql, String queue, String jobName, int numMappers, String jdbcUrl) {
        List<String> cmds = new ArrayList<>();
        cmds.add("eval");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(jdbcUrl);
        cmds.add("--query");
        cmds.add(sql);
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(yarnConfiguration));
    }

}
