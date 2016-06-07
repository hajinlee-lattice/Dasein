package com.latticeengines.dellebi.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.service.FileFlowService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;
import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;

@Component("dellEbiFlowService")
public class DellEbiFlowServiceImpl implements DellEbiFlowService {

    private static final Log log = LogFactory.getLog(DailyFlow.class);
    static final int FAIL_TRIES = 3;
    @Resource(name = "localFileFlowService")
    private FileFlowService localFileFlowService;

    @Resource(name = "smbFileFlowService")
    private FileFlowService smbFileFlowService;

    @Autowired
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    @Value("${dellebi.output.table.sample}")
    private String targetTable;

    @Autowired
    private DellEbiExecutionLogEntityMgr dellEbiExecutionLogEntityMgr;

    @Autowired
    private JdbcTemplate dellEbiTargetJDBCTemplate;

    @Override
    public DataFlowContext getFile(DataFlowContext context) {

        if (!readConfigs(context)) {
            context.setProperty(RESULT_KEY, false);
            log.error("There is no type entry defined in the config table");
            return context;
        } else {
            context.setProperty(RESULT_KEY, true);
        }

        smbFileFlowService.initialContext(context);

        String fileName = context.getProperty(ZIP_FILE_NAME, String.class);

        if (fileName != null) {
            return context;
        }

        localFileFlowService.initialContext(context);

        return context;
    }

    private boolean isSmb(DataFlowContext context) {
        if (context == null) {
            return true;
        }
        String smb = context.getProperty(FILE_SOURCE, String.class);
        if (FILE_SOURCE_SMB.equals(smb)) {
            return true;
        }
        return false;
    }

    @Override
    public String getOutputDir(DataFlowContext context) {
        String type = context.getProperty(FILE_TYPE, String.class);
        if (isSmb(context)) {
            return smbFileFlowService.getOutputDir(type);
        }
        return localFileFlowService.getOutputDir(type);
    }

    @Override
    public String getTxtDir(DataFlowContext context) {
        String type = context.getProperty(FILE_TYPE, String.class);
        if (isSmb(context)) {
            return smbFileFlowService.getTxtDir(type);
        }
        return localFileFlowService.getTxtDir(type);
    }

    @Override
    public String getZipDir(DataFlowContext context) {
        if (isSmb(context)) {
            return smbFileFlowService.getZipDir();
        }
        return localFileFlowService.getZipDir();
    }

    @Override
    public String getFileType(DataFlowContext context) {
        String zipFileName = context.getProperty(ZIP_FILE_NAME, String.class);
        if (isSmb(context)) {
            return smbFileFlowService.getFileType(zipFileName);
        }
        return localFileFlowService.getFileType(zipFileName);
    }

    @Override
    public String getTargetDB(DataFlowContext context) {
        String type = context.getProperty(DellEbiFlowService.FILE_TYPE, String.class);
        if (isSmb(context)) {
            return smbFileFlowService.getTargetDB(type);
        }
        return localFileFlowService.getTargetDB(type);
    }

    @Override
    public boolean deleteFile(DataFlowContext context) {
        String zipFileName = context.getProperty(ZIP_FILE_NAME, String.class);
        Boolean isDeleted;
        String fileType;

        fileType = getFileType(context);

        isDeleted = dellEbiConfigEntityMgr.getIsDeleted(fileType);

        if (isDeleted == null || isDeleted == false)
            return true;

        if (isSmb(context)) {
            return smbFileFlowService.deleteFile(zipFileName);
        }
        return localFileFlowService.deleteFile(zipFileName);
    }

    @Override
    public void registerFailedFile(DataFlowContext context, String err) {

        String zipFileName = context.getProperty(ZIP_FILE_NAME, String.class);

        DellEbiExecutionLog dellEbiExecutionLog = context.getProperty(LOG_ENTRY, DellEbiExecutionLog.class);

        List<DellEbiExecutionLog> executionLogs = dellEbiExecutionLogEntityMgr.getEntriesByFile(zipFileName);
        int count = 0;
        for (DellEbiExecutionLog executionLog : executionLogs) {
            if (executionLog.getStatus() == DellEbiExecutionLogStatus.Completed.getStatus()) {
                break;
            } else {
                count++;
            }
        }

        if (count >= FAIL_TRIES) {
            dellEbiExecutionLogEntityMgr.recordRetryFailure(dellEbiExecutionLog, err);
            log.info("Failed to re-try file name=" + zipFileName);
        } else {
            dellEbiExecutionLogEntityMgr.recordFailure(dellEbiExecutionLog, err);
        }
    }

    @Override
    public boolean runStoredProcedure(DataFlowContext context) {
        String type = context.getProperty(FILE_TYPE, String.class);
        String spName = dellEbiConfigEntityMgr.getPostStoreProcedure(type);
        if (isSmb(context) && spName != null) {
            String sqlStr = "exec " + spName;
            log.info("Begin to execute the Store Procedure= " + spName);
            dellEbiTargetJDBCTemplate.execute(sqlStr);
            log.info("Finished executing the Store Procedure= " + spName);
            return true;
        }
        return false;
    }

    @Override
    public String getErrorOutputDir(DataFlowContext context) {
        if (isSmb(context)) {
            return smbFileFlowService.getErrorOutputDir();
        }
        return localFileFlowService.getErrorOutputDir();
    }

    @Override
    public String getTargetColumns(DataFlowContext context) {
        String fileType = context.getProperty(FILE_TYPE, String.class);

        return dellEbiConfigEntityMgr.getTargetColumns(fileType);
    }

    @Override
    public String getTargetTable(DataFlowContext context) {
        String fileType = context.getProperty(FILE_TYPE, String.class);

        return dellEbiConfigEntityMgr.getTargetTable(fileType);
    }

    private Boolean readConfigs(DataFlowContext context) {

        dellEbiConfigEntityMgr.initialService();

        String[] types = context.getProperty(TYPES_LIST, String[].class);
        List<DellEbiConfig> configsList = new ArrayList<DellEbiConfig>();

        for (String type : types) {
            type = type.trim();
            DellEbiConfig config = dellEbiConfigEntityMgr.getConfigByType(type);
            if (config != null) {
                log.info("The configuration for: " + type + " is " + config.toString());
                configsList.add(config);
            }
        }

        if (configsList.size() == 0) {
            return false;
        } else {
            context.setProperty(DellEbiFlowService.CFG_LIST, configsList);
            return true;
        }
    }

    public static <T> List<T> asList(Collection<?> c, Class<? extends T> type) {
        if (c == null)
            return null;
        List<T> list = new ArrayList<T>(c.size());
        for (Object o : c)
            list.add(type.cast(o));
        return list;
    }
}
