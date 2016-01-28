package com.latticeengines.liaison.service.impl;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
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
        ConnectionMgr conn_mgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlURL);

        String source = "OrbIntelligence_Source";
        Set<String> ldcCols = new HashSet<>();
        ldcCols.add("linkedin_url");
        ldcCols.add("BusinessName");

        AbstractMap.SimpleImmutableEntry<String, String> typeAndVersion = lpfcn.getLPTemplateTypeAndVersion(conn_mgr);
        String lp_template_type = typeAndVersion.getKey();
        String lp_template_version = typeAndVersion.getValue();

        Map<String, String> existingAtts = lpfcn.getLDCWritebackAttributes(conn_mgr, source, lp_template_version);
        for (Map.Entry<String, String> att : existingAtts.entrySet()) {
            System.out.println(String.format("%s --> %s", att.getKey(), att.getValue()));
        }

        lpfcn.removeLDCWritebackAttributes(conn_mgr, lp_template_version);
        lpfcn.addLDCMatch(conn_mgr, source, lp_template_version);
        lpfcn.setLDCWritebackAttributesDefaultName(conn_mgr, source, ldcCols, lp_template_type, lp_template_version);
        conn_mgr.getLoadGroupMgr().commit();
    }

}
