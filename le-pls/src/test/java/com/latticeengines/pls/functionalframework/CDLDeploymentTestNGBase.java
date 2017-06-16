package com.latticeengines.pls.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;

public abstract class CDLDeploymentTestNGBase extends PlsDeploymentTestNGBase {

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    protected String dataCollectionName;
    protected String customerSpace;

    public void setupTenant() {
        testBed.bootstrapForProduct(LatticeProduct.LPA3);
        customerSpace = CustomerSpace.parse(testBed.getMainTestTenant().getId()).toString();
        DataCollection dataCollection = new DataCollection();
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollection = dataCollectionProxy.createOrUpdateDataCollection(customerSpace, dataCollection);
        dataCollectionName = dataCollection.getName();
    }
}
