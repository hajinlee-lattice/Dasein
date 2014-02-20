package com.latticeengines.dataplatform.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.domain.DataSchema;
import com.latticeengines.dataplatform.exposed.domain.DbCreds;
import com.latticeengines.dataplatform.exposed.domain.Field;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.dataplatform.service.impl.metadata.MetadataProvider;

@Component("metadataService")
public class MetadataServiceImpl implements MetadataService {

    private Map<String, MetadataProvider> metadataProviders;

    @Autowired
    public void setMetadataProviders(
            @Value("#{metadataProviders}") Map<String, MetadataProvider> metadataProviders) {
        this.metadataProviders = metadataProviders;
    }

    @Override
    public DataSchema createDataSchema(DbCreds creds, String tableName) {
        DataSchema dataSchema = new DataSchema();
        MetadataProvider provider = metadataProviders.get("SQLServer");
        Connection conn = provider.getConnection(creds);
        String query = "SELECT * FROM " + tableName;
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSet rs = pstmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                Field field = new Field();
                String columnName = rsmd.getColumnName(i);
                String dbType = rsmd.getColumnTypeName(i);

                List<String> typeInfo = Arrays.<String> asList(new String[] {
                        provider.getType(dbType),
                        provider.getDefaultValue(dbType) });
                field.setName(columnName);
                field.setType(typeInfo);
                dataSchema.addField(field);
            }
        } catch (SQLException e) {
            throw new LedpException(LedpCode.LEDP_11002, e,
                    new String[] { query });
        }
        return dataSchema;
    }

}
