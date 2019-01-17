package com.latticeengines.apps.cdl.provision.impl;

import java.util.List;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.proxy.exposed.component.ComponentProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

public class CDLComponentServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CDLComponentServiceImplDeploymentTestNG.class);

    @Inject
    private ComponentProxy componentProxy;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private Configuration yarnConfiguration;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void testResetTenant() throws Exception {
        checkpointService.resumeCheckpoint("process2", 22);

        List<DataUnit> dataUnits = dataUnitProxy.getByStorageType(mainCustomerSpace, DataUnit.StorageType.Redshift);
        Assert.assertTrue(dataUnits.size() > 0);

        CustomerSpace cs = CustomerSpace.parse(mainCustomerSpace);
        String path = PathBuilder.buildDataTableSchemaPath(podId, cs).toString();

        List<String> files = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, path, null);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, path));
        Assert.assertTrue(files.size() > 0);

        componentProxy.reset(mainCustomerSpace, ComponentConstants.CDL);
        componentProxy.reset(mainCustomerSpace, ComponentConstants.LP);
        componentProxy.reset(mainCustomerSpace, ComponentConstants.METADATA);

        dataUnits = dataUnitProxy.getByStorageType(mainCustomerSpace, DataUnit.StorageType.Redshift);
        Assert.assertEquals(dataUnits.size(), 0);

        Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, path));
    }
}
