package com.latticeengines.liaison.service.impl;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.vdb.ParseTree;
import com.latticeengines.common.exposed.vdb.SpecParseException;
import com.latticeengines.common.exposed.vdb.SpecParser;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.testframework.LiaisonTestNGBase;

public class ConnectionMgrFactoryImplTestNG extends LiaisonTestNGBase {

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Test(groups = "functional")
    public void testConnection() {
        String tenantName = "Internal_LP_SFDC_220_2";
        String dlURL = "https://data-pls2.prod.lattice.local/dataloader/";

        String queryName = "Q_PLS_Scoring_Incremental";

        ConnectionMgr connectionMgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlURL);

        try {
            // Get the query
            Query q = connectionMgr.getQuery(queryName);
            String spec = q.getOriginalDefinition();
            SpecParser sp = new SpecParser(spec);
            sp.parse();
            ParseTree parseTree = sp.getParseTree();
            parseTree.printParseTree();
        } catch (IOException | SpecParseException ex) {
            System.out.println(String.format("Exception: %s", ex.getMessage()));
        }

    }

}
