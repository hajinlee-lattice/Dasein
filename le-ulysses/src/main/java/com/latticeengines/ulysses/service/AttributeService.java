package com.latticeengines.ulysses.service;

import java.util.List;

import com.latticeengines.domain.exposed.attribute.PrimaryField;
import com.latticeengines.domain.exposed.validation.ValidationExpression;

public interface AttributeService {

    List<PrimaryField> getPrimaryFields();

    ValidationExpression getPrimaryFieldValidationExpression();

}
