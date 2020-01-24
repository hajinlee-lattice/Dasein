package com.latticeengines.dataplatform.service.impl.dlorchestration;

import javax.inject.Inject;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@Component("debugProcessorImpl")
public class DebugProcessorImpl {

    public static final String COPY_SUFFIX = "_copy";

    @Inject
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    @Inject
    private ModelCommandLogService modelCommandLogService;

    public void execute(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        try {
            String dbCopy = modelCommand.getEventTable() + COPY_SUFFIX;
            String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData()
                    .getDriverName();
            if (dbDriverName.contains("Microsoft")) {
                dlOrchestrationJdbcTemplate.execute("IF OBJECT_ID('" + dbCopy + "', 'U') IS NOT NULL DROP TABLE "
                        + dbCopy);
                dlOrchestrationJdbcTemplate
                        .execute("select * into " + dbCopy + " from " + modelCommand.getEventTable());
            } else {
                dlOrchestrationJdbcTemplate.execute("drop table if exists " + dbCopy);
                dlOrchestrationJdbcTemplate.execute("CREATE TABLE " + dbCopy + " LIKE " + modelCommand.getEventTable());
                dlOrchestrationJdbcTemplate.execute("INSERT " + dbCopy + " SELECT * FROM "
                        + modelCommand.getEventTable());
            }
            modelCommandLogService.log(modelCommand, "Created copy of event table into: " + dbCopy);
        } catch (Exception e) {
            modelCommandLogService.logException(modelCommand, "Problem creating copy of event table", e);
        }
    }

    public JdbcTemplate getDlOrchestrationJdbcTemplate() {
        return dlOrchestrationJdbcTemplate;
    }
}
