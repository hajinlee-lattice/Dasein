package com.latticeengines.dataplatform.service.impl.dlorchestration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@Component("debugProcessorImpl")
public class DebugProcessorImpl {

    private static final Log log = LogFactory.getLog(DebugProcessorImpl.class);
    public static final String COPY_SUFFIX = "_copy";

    @Autowired
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    public void execute(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        try {
            String dbCopy = modelCommandParameters.getEventTable() + COPY_SUFFIX;
            String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData()
                    .getDriverName();
            if (dbDriverName.contains("Microsoft")) {
                dlOrchestrationJdbcTemplate.execute("select * into " + dbCopy + " from "
                        + modelCommandParameters.getEventTable());
            } else {
                dlOrchestrationJdbcTemplate.execute("CREATE TABLE " + dbCopy + " LIKE "
                        + modelCommandParameters.getEventTable());
                dlOrchestrationJdbcTemplate.execute("INSERT " + dbCopy + " SELECT * FROM "
                        + modelCommandParameters.getEventTable());
            }
        } catch (Exception e) {
            log.error("Problem creating copy of event table", e);
        }
    }
}
