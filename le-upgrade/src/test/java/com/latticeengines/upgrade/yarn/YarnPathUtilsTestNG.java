package com.latticeengines.upgrade.yarn;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class YarnPathUtilsTestNG extends UpgradeFunctionalTestNGBase {

    private String path;

    @BeforeClass(groups = {"unit", "functional"})
    public void setup() {
        path = "/user/s-analytics/customers";
        path += "/Nutanix_PLS132/models";
        path += "/Q_PLS_Modeling_Nutanix_PLS132";
        path += "/5d074f72-c8f0-4d53-aebc-912fb066daa0";
        path += "/1416355548818_20011";
    }

    @Test(groups = {"unit", "functional"})
    public void testParseEventTable() throws Exception {
        String eventTable = YarnPathUtils.parseEventTable(path);
        Assert.assertEquals(eventTable, EVENT_TABLE);
    }

    @Test(groups = {"unit", "functional"})
    public void testParseContainerId() throws Exception {
        String containerId = YarnPathUtils.parseContainerId(path);
        Assert.assertEquals(containerId, CONTAINER_ID);
    }

    @Test(groups = {"unit", "functional"})
    public void testParseUuid() throws Exception {
        String uuid = YarnPathUtils.parseUuid(path);
        Assert.assertEquals(uuid, UUID);
    }

    @Test(groups = {"unit", "functional"})
    public void testExtractUuid() throws Exception {
        String uuid = YarnPathUtils.extractUuid(MODEL_GUID);
        Assert.assertEquals(uuid, UUID);
    }

    @Test(groups = {"unit", "functional"})
    public void testSubstituteIds() throws Exception {
        String singluarPath = "hdfs://localhost:9000/user/s-analytics/customers/Lattice_Relaunch/models";
        singluarPath += "/Q_PLS_Modeling_Lattice_Relaunch/b99ddcc6-7ecb-45a0-b128-9664b51c1ce9";
        singluarPath += "/1425511391553_3443/enhancements/modelsummary.json";

        String tuplePath = "hdfs://localhost:9000/user/s-analytics/customers/";
        tuplePath += "Lattice_Relaunch.Lattice_Relaunch.Production/models";
        tuplePath += "/Q_PLS_Modeling_Lattice_Relaunch/b99ddcc6-7ecb-45a0-b128-9664b51c1ce9";
        tuplePath += "/1425511391553_3443/enhancements/modelsummary.json";

        Assert.assertEquals(YarnPathUtils.substituteByTupleId(singluarPath), tuplePath);
        Assert.assertEquals(YarnPathUtils.substituteByTupleId(tuplePath), tuplePath);
        Assert.assertEquals(YarnPathUtils.substituteBySingularId(singluarPath), singluarPath);
        Assert.assertEquals(YarnPathUtils.substituteBySingularId(tuplePath), singluarPath);
    }

    @Test(groups = {"unit", "functional"})
    public void testConstructPath() throws Exception {
        String customerRoot = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, CUSTOMER);
        Assert.assertEquals(customerRoot, "/user/s-analytics/customers/" + TUPLE_ID);

        String modelsRoot = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER);
        Assert.assertEquals(modelsRoot,
                "/user/s-analytics/customers/" + TUPLE_ID + "/models");

        String dataRoot = YarnPathUtils.constructTupleIdDataRoot(customerBase, CUSTOMER);
        Assert.assertEquals(dataRoot,
                "/user/s-analytics/customers/" + TUPLE_ID + "/data");

        customerRoot = YarnPathUtils.constructSingularIdCustomerRoot(customerBase, CUSTOMER);
        Assert.assertEquals(customerRoot, "/user/s-analytics/customers/" + CUSTOMER);

        modelsRoot = YarnPathUtils.constructSingularIdModelsRoot(customerBase, CUSTOMER);
        Assert.assertEquals(modelsRoot, "/user/s-analytics/customers/" + CUSTOMER + "/models");

        dataRoot = YarnPathUtils.constructSingularIdDataRoot(customerBase, CUSTOMER);
        Assert.assertEquals(dataRoot, "/user/s-analytics/customers/" + CUSTOMER + "/data");
    }

}


