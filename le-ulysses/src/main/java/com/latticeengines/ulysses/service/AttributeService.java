package com.latticeengines.ulysses.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.attribute.PrimaryField;
import com.latticeengines.domain.exposed.ulysses.PrimaryFieldValidationExpression;

public interface AttributeService {

    List<PrimaryField> getPrimaryFields();

    PrimaryFieldValidationExpression getPrimaryFieldValidationExpression(CustomerSpace customerSpace);

    List<PrimaryField> getPrimaryFieldsFromExternalSystem(CustomerSpace customerSpace, String type);

    Map<String, List<PrimaryField>> getPrimaryFieldsFromExternalSystem(CustomerSpace customerSpace);

}
