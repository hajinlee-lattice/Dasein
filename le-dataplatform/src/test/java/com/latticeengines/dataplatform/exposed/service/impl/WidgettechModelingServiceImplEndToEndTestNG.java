package com.latticeengines.dataplatform.exposed.service.impl;

import org.testng.annotations.AfterClass;


public class WidgettechModelingServiceImplEndToEndTestNG extends ModelingServiceImplUnpivotedEndToEndTestNG {

    @Override
    public String getCustomer() {
        return "Widgettech.Widgettech.Development";
    }
    
    @AfterClass(groups = "functional")
    public void tearDown() {
    }
}
