package com.latticeengines.propdata.core.datasource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class DataSourceUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testGetDataSource() throws SQLException, IOException {
        InputStream is =
                Thread.currentThread().getContextClassLoader().getResourceAsStream("datasource/test_source_dbs.json");
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.readValue(is, ArrayNode.class);
        for (JsonNode node: arrayNode) {
            DataSourceConnection dataSourceConnection = mapper.treeToValue(node, DataSourceConnection.class);
            DriverManagerDataSource dataSource = DataSourceUtils.getDataSource(dataSourceConnection);
            Connection connection = dataSource.getConnection();
            connection.close();
        }
    }

}
