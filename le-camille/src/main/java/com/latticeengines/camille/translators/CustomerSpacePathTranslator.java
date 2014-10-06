package com.latticeengines.camille.translators;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;

public class CustomerSpacePathTranslator extends PathTranslator {
    private CustomerSpaceScope scope;
    
    public CustomerSpacePathTranslator(CustomerSpaceScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getAbsolutePath(Path p) {
        // TODO Auto-generated method stub
        return null;
    }
}
