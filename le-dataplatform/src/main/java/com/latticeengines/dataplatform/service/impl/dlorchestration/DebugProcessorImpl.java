package com.latticeengines.dataplatform.service.impl.dlorchestration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@Component("debugProcessorImpl")
public class DebugProcessorImpl {

    public static final String COPY_SUFFIX = "_copy";

    @Autowired
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    @Autowired
    private ModelCommandLogService modelCommandLogService;

    public void execute(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        try {
            String dbCopy = modelCommandParameters.getEventTable() + COPY_SUFFIX;
            String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData()
                    .getDriverName();
            if (dbDriverName.contains("Microsoft")) {
                dlOrchestrationJdbcTemplate.execute("IF OBJECT_ID('" + dbCopy + "', 'U') IS NOT NULL DROP TABLE "
                        + dbCopy);
                dlOrchestrationJdbcTemplate.execute("select * into " + dbCopy + " from "
                        + modelCommandParameters.getEventTable());
            } else {
                dlOrchestrationJdbcTemplate.execute("drop table if exists " + dbCopy);
                dlOrchestrationJdbcTemplate.execute("CREATE TABLE " + dbCopy + " LIKE "
                        + modelCommandParameters.getEventTable());
                dlOrchestrationJdbcTemplate.execute("INSERT " + dbCopy + " SELECT * FROM "
                        + modelCommandParameters.getEventTable());
            }
            modelCommandLogService.log(modelCommand, "Created copy of event table into: " + dbCopy);
        } catch (Exception e) {
            modelCommandLogService.logException(modelCommand, "Problem creating copy of event table", e);
        }
    }
    
    public JdbcTemplate getDlOrchestrationJdbcTemplate(){
        return dlOrchestrationJdbcTemplate;
    }
}
