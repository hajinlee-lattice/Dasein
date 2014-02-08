package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.domain.DataSchema;
import com.latticeengines.dataplatform.exposed.domain.DbCreds;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.MetadataService;

public class MetadataServiceImplTestNG extends DataPlatformFunctionalTestNGBase {
	
	@Autowired
	private MetadataService metadataService;
	
	@Override
	protected boolean doYarnClusterSetup() {
		return false;
	}

	@Test(groups = "functional")
	public void createDataSchema() {
		DbCreds.Builder builder = new DbCreds.Builder();
		builder.host("rgonzalez-vbox.lattice.local") //
			.db("ledp") //
			.port(1433) //
			.user("sa") //
			.password("Welcome123");
		
		DbCreds creds = new DbCreds(builder);
		
		DataSchema schema = metadataService.createDataSchema(creds, "all_datatypes");
		assertEquals(schema.getFields().size(), 34);
	}
}
