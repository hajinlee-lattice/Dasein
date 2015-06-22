package com.latticeengines.upgrade.yarn;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class YarnPathUtilsUnitTestNG {
    private static final String CUSTOMER_BASE = "/user/s-analytics/customers";
    private static final String CUSTOMER = "Lattice_Relaunch";

    private String path;

    @BeforeClass(groups = {"unit", "functional"})
    public void setup() {
        path = "/user/s-analytics/customers";
        path += "/Lattice_Relaunch/models";
        path += "/Q_PLS_Modeling_Lattice_Relaunch/b99ddcc6-7ecb-45a0-b128-9664b51c1ce9/1425511391553_3443";
    }

    @Test(groups = {"unit", "functional"})
    public void testParseEventTable() throws Exception {
        String eventTable = YarnPathUtils.parseEventTable(path);
        Assert.assertEquals(eventTable, "Q_PLS_Modeling_Lattice_Relaunch");
    }

    @Test(groups = {"unit", "functional"})
    public void testParseContainerId() throws Exception {
        String eventTable = YarnPathUtils.parseContainerId(path);
        Assert.assertEquals(eventTable, "1425511391553_3443");
    }

    @Test(groups = {"unit", "functional"})
    public void testParseModelGuid() throws Exception {
        String eventTable = YarnPathUtils.parseModelGuid(path);
        Assert.assertEquals(eventTable, "ms__b99ddcc6-7ecb-45a0-b128-9664b51c1ce9-PLSModel");
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
        String customerRoot = YarnPathUtils.constructTupleIdCustomerRoot(CUSTOMER_BASE, CUSTOMER);
        Assert.assertEquals(customerRoot, "/user/s-analytics/customers/Lattice_Relaunch.Lattice_Relaunch.Production");

        String modelsRoot = YarnPathUtils.constructTupleIdModelsRoot(CUSTOMER_BASE, CUSTOMER);
        Assert.assertEquals(modelsRoot,
                "/user/s-analytics/customers/Lattice_Relaunch.Lattice_Relaunch.Production/models");

        String dataRoot = YarnPathUtils.constructTupleIdDataRoot(CUSTOMER_BASE, CUSTOMER);
        Assert.assertEquals(dataRoot,
                "/user/s-analytics/customers/Lattice_Relaunch.Lattice_Relaunch.Production/data");

        customerRoot = YarnPathUtils.constructSingularIdCustomerRoot(CUSTOMER_BASE, CUSTOMER);
        Assert.assertEquals(customerRoot, "/user/s-analytics/customers/Lattice_Relaunch");

        modelsRoot = YarnPathUtils.constructSingularIdModelsRoot(CUSTOMER_BASE, CUSTOMER);
        Assert.assertEquals(modelsRoot, "/user/s-analytics/customers/Lattice_Relaunch/models");

        dataRoot = YarnPathUtils.constructSingularIdDataRoot(CUSTOMER_BASE, CUSTOMER);
        Assert.assertEquals(dataRoot, "/user/s-analytics/customers/Lattice_Relaunch/data");
    }

}


