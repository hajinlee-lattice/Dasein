package com.latticeengines.metadata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;

import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class DataCollectionResourceTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

}
