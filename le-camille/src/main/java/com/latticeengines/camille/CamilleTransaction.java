package com.latticeengines.camille;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.zookeeper.data.ACL;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

/**
 * Simple wrapper over CuratorTransaction
 */
public class CamilleTransaction {
    private CuratorTransaction transaction;
        
    public CamilleTransaction() {
        CuratorFramework curator = CamilleEnvironment.getCamille().getCuratorClient();
        this.transaction = curator.inTransaction();
    }
    
    public void check(Path path, Document document) {
        // TODO
    }
    
    public void create(Path path, Document document, List<ACL> acl) {
        // TODO
    }
   
    public void set(Path path, Document document) {
        // TODO
    }
    
    public void delete(Path path) {
        // TODO
    }
    
    public void commit() {
        // TODO
    }
}
