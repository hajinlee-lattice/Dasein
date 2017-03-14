package com.latticeengines.ulysses.service;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.ulysses.PrimaryField;
import com.latticeengines.domain.exposed.ulysses.PrimaryFieldValidationExpression;

public interface AttributeService {

    List<PrimaryField> getPrimaryFields();

    PrimaryFieldValidationExpression getPrimaryFieldValidationExpression(CustomerSpace customerSpace);

}
