package com.latticeengines.domain.exposed.metadata.validators;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Table;

public abstract class InputValidator {
    public abstract boolean validate(String field, Map<String, String> row, Table metadata);
}
