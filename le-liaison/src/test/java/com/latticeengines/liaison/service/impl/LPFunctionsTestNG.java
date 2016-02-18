package com.latticeengines.liaison.service.impl;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.liaison.exposed.service.LPFunctions;
import com.latticeengines.liaison.testframework.LiaisonTestNGBase;

public class LPFunctionsTestNG extends LiaisonTestNGBase {

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Autowired
    private LPFunctions lpfcn;

    @Test(groups = "functional")
    public void testLPFunctions() throws IOException {

        String tenantName = "Internal_LP_SFDC_220_2";
        String dlURL = "https://data-pls2.prod.lattice.local/dataloader/";
        //String tenantName = "TestSFDC_20160202";
        //String dlURL = "https://10.41.1.187:8080/";
        ConnectionMgr conn_mgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlURL);

        AbstractMap.SimpleImmutableEntry<String, String> typeAndVersions = lpfcn.getLPTemplateTypeAndVersion(conn_mgr);

        Map<String, Set<String>> map = new HashMap<String, Set<String>>();
        Set<String> columns1 = new HashSet<String>();
        map.put("OrbIntelligence_Source", columns1);
        columns1.add("BusinessAddress1");

        Set<String> columns2 = new HashSet<String>();
        map.put("BuiltWith_Pivoted_Source", columns2);
        columns2.add("TechIndicator_Akamai");

        Set<String> columns3 = new HashSet<String>();
        map.put("HGData_Pivoted_Source", columns3);
        columns3.add("TechIndicator_ADP");

        //String source = "OrbIntelligence_Source";
        //Set<String> ldcCols = new HashSet<>();
        //ldcCols.add("linkedin_url");
        //ldcCols.add("BusinessName");
        //ldcCols.add("BusinessAddress1");

        for (Entry<String, Set<String>> entry : map.entrySet()) {
            String source = entry.getKey();
            lpfcn.addLDCMatch(conn_mgr, source, typeAndVersions.getValue());
            lpfcn.setLDCWritebackAttributesDefaultName(conn_mgr, source, entry.getValue(),
                    typeAndVersions.getKey(), typeAndVersions.getValue());
        }

        //Map<String, String> existingAtts = lpfcn.getLDCWritebackAttributes(conn_mgr, source, typeAndVersions.getValue());
        //for (Map.Entry<String, String> att : existingAtts.entrySet()) {
        //    System.out.println(String.format("%s --> %s", att.getKey(), att.getValue()));
        // }

        //System.out.println("\nGetting Table\n");
        //Map<String, String> mapTableAndDataProvider = lpfcn.getTargetTablesAndDataProviders(conn_mgr, typeAndVersions.getValue());
        //for (Map.Entry<String, String> tableAndDataProvider : mapTableAndDataProvider.entrySet()) {
        //    System.out.println(String.format("%s --> %s", tableAndDataProvider.getKey(), tableAndDataProvider.getValue()));
        //}

        //lpfcn.removeLDCWritebackAttributes(conn_mgr, lp_template_version);
        //lpfcn.addLDCMatch(conn_mgr, source, lp_template_version);
        //lpfcn.setLDCWritebackAttributesDefaultName(conn_mgr, source, ldcCols, lp_template_type, lp_template_version);
        conn_mgr.getLoadGroupMgr().commit();
    }

}
