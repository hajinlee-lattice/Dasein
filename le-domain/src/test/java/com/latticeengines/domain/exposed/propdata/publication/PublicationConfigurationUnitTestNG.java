package com.latticeengines.domain.exposed.propdata.publication;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class PublicationConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {

        PublishToSqlConfiguration configuration = new PublishToSqlConfiguration();
        configuration.setDefaultTableName("DefaultTable");

        SqlDestination destination = new SqlDestination();
        destination.setTableName("Table1");
        configuration.setDestination(destination);

        String json = JsonUtils.serialize(configuration);

        PublicationConfiguration deser = JsonUtils.deserialize(json, PublicationConfiguration.class);
        Assert.assertTrue(deser instanceof PublishToSqlConfiguration);
        Assert.assertTrue(deser.getDestination() instanceof SqlDestination);
    }

}
