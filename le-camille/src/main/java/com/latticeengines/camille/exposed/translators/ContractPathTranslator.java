package com.latticeengines.camille.exposed.translators;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;

public class ContractPathTranslator extends PathTranslator {
    private ContractScope scope;

    public ContractPathTranslator(ContractScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getBasePath() throws Exception {
        return PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), scope.getContractId());
    }
}
