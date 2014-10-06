package com.latticeengines.camille.translators;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;

public class ContractPathTranslator extends PathTranslator {
    private ContractScope scope;

    public ContractPathTranslator(ContractScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getAbsolutePath(Path p) throws Exception {
        return PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), scope.getContractID()).append(p);
    }
}
