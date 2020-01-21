package com.latticeengines.pls.functionalframework;

import javax.inject.Inject;

import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

public abstract class CDLDeploymentTestNGBase extends PlsDeploymentTestNGBase {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    protected String dataCollectionName;
    protected String customerSpace;

    public void setupTenant() {
//        testBed.bootstrapForProduct(LatticeProduct.LPA3);
//        customerSpace = CustomerSpace.parse(testBed.getMainTestTenant().getId()).toString();
//        DataCollection dataCollection = new DataCollection();
//        dataCollection.setType(DataCollectionType.Segmentation);
//        dataCollection = dataCollectionProxy.createOrUpdateDataCollection(customerSpace, dataCollection);
//        dataCollectionName = dataCollection.getName();
    }
}
