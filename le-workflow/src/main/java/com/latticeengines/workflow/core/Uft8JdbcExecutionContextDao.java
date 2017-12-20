package com.latticeengines.workflow.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.hsqldb.types.Charset;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.JdbcExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.RowMapper;

public class Uft8JdbcExecutionContextDao extends JdbcExecutionContextDao {

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
            results = new String(out.toByteArray(), Charset.UTF8.toString());
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Could not serialize the execution context", ioe);
        }

        return results;
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
                ByteArrayInputStream in = new ByteArrayInputStream(serializedContext.getBytes(Charset.UTF8.toString()));
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
