package com.latticeengines.camille.translators;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServicePathTranslator extends PathTranslator {
    private CustomerSpaceServiceScope scope;

    public CustomerSpaceServicePathTranslator(CustomerSpaceServiceScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getAbsolutePath(Path p) {
        // TODO Auto-generated method stub
        return null;
    }
}
