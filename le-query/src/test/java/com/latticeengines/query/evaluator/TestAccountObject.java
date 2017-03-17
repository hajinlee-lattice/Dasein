package com.latticeengines.query.evaluator;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.query.exposed.object.BusinessObject;

@Component
public class TestAccountObject extends BusinessObject {
    @Override
    public SchemaInterpretation getObjectType() {
        return SchemaInterpretation.Account;
    }
}
