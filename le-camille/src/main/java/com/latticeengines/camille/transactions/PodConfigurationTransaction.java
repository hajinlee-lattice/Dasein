package com.latticeengines.camille.transactions;

import com.latticeengines.camille.ConfigurationTransaction;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;

public class PodConfigurationTransaction extends ConfigurationTransaction {
    private PodScope scope;

    public PodConfigurationTransaction(PodScope scope) {
        this.scope = scope;
    }

    @Override
    public void check(Path path, Document document) {
        // TODO Auto-generated method stub

    }

    @Override
    public void create(Path path, Document document) {
        // TODO Auto-generated method stub

    }

    @Override
    public void set(Path path, Document document) {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(Path path) {
        // TODO Auto-generated method stub

    }

    @Override
    public void commit() {
        // TODO Auto-generated method stub

    }

}
