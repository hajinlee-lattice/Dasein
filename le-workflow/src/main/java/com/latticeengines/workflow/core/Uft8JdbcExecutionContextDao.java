package com.latticeengines.workflow.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.JdbcExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

public class Uft8JdbcExecutionContextDao extends JdbcExecutionContextDao {

    private static final String FIND_JOB_EXECUTION_CONTEXT = "SELECT SHORT_CONTEXT, SERIALIZED_CONTEXT "
            + "FROM %PREFIX%JOB_EXECUTION_CONTEXT WHERE JOB_EXECUTION_ID = ?";

    private static final String FIND_STEP_EXECUTION_CONTEXT = "SELECT SHORT_CONTEXT, SERIALIZED_CONTEXT "
            + "FROM %PREFIX%STEP_EXECUTION_CONTEXT WHERE STEP_EXECUTION_ID = ?";

    private ExecutionContextSerializer serializer;

    public void setSerializer(ExecutionContextSerializer serializer) {
        this.serializer = serializer;
    }

    private String serializeContext(ExecutionContext ctx) {
        Map<String, Object> m = new HashMap<String, Object>();
        for (Entry<String, Object> me : ctx.entrySet()) {
            m.put(me.getKey(), me.getValue());
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String results = "";

        try {
            serializer.serialize(m, out);
            results = new String(out.toByteArray(), Charset.forName("UTF-8"));
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Could not serialize the execution context", ioe);
        }

        return results;
    }

    @Override
    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        Long executionId = jobExecution.getId();
        Assert.notNull(executionId, "ExecutionId must not be null.");

        List<ExecutionContext> results = getJdbcTemplate().query(getQuery(FIND_JOB_EXECUTION_CONTEXT),
                new ExecutionContextRowMapper(), executionId);
        if (results.size() > 0) {
            return results.get(0);
        } else {
            return new ExecutionContext();
        }
    }

    @Override
    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        Long executionId = stepExecution.getId();
        Assert.notNull(executionId, "ExecutionId must not be null.");

        List<ExecutionContext> results = getJdbcTemplate().query(getQuery(FIND_STEP_EXECUTION_CONTEXT),
                new ExecutionContextRowMapper(), executionId);
        if (results.size() > 0) {
            return results.get(0);
        } else {
            return new ExecutionContext();
        }
    }

    private class ExecutionContextRowMapper implements RowMapper<ExecutionContext> {

        @Override
        public ExecutionContext mapRow(ResultSet rs, int i) throws SQLException {
            ExecutionContext executionContext = new ExecutionContext();
            String serializedContext = rs.getString("SERIALIZED_CONTEXT");
            if (serializedContext == null) {
                serializedContext = rs.getString("SHORT_CONTEXT");
            }

            Map<String, Object> map;
            try {
                ByteArrayInputStream in = new ByteArrayInputStream(serializedContext.getBytes(Charset.forName("UTF-8")));
                map = serializer.deserialize(in);
            } catch (IOException ioe) {
                throw new IllegalArgumentException("Unable to deserialize the execution context", ioe);
            }
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                executionContext.put(entry.getKey(), entry.getValue());
            }
            return executionContext;
        }
    }
}
