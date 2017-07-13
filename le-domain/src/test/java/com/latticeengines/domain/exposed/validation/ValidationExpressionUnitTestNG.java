package com.latticeengines.domain.exposed.validation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ValidationExpressionUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(ValidationExpressionUnitTestNG.class);
    
    private static final ObjectMapper OM = new ObjectMapper();

    @Test(groups = "unit")
    public void testTwoFields() throws IOException {
        ValidationExpression exp = new ValidationExpression(Condition.OR, new ValidationField("website"),
                new ValidationField("email"));
        validateJsonMapping(exp);
    }

    @Test(groups = "unit")
    public void testTwoExpressions() throws IOException {
        ValidationExpression innerExp1 = getCompanyStateExpression();
        ValidationExpression innerExp2 = getCompanyCountryExpression();
        ValidationExpression exp = new ValidationExpression(Condition.OR, innerExp1, innerExp2);
        validateJsonMapping(exp);
    }

    @Test(groups = "unit")
    public void testExpressionWithFields() throws IOException {
        ValidationExpression innerExp = getCompanyStateExpression();
        ValidationExpression exp = new ValidationExpression(Condition.OR, innerExp, new ValidationField("website"),
                new ValidationField("email"));
        validateJsonMapping(exp);
    }

    @Test(groups = "unit")
    public void testExpressionsAndFields() throws IOException {
        ValidationExpression innerExp1 = getCompanyStateExpression();
        ValidationExpression innerExp2 = getCompanyCountryExpression();
        List<ValidationExpression> expressionLst = new ArrayList<>();
        expressionLst.add(innerExp1);
        expressionLst.add(innerExp2);
        ValidationExpression exp = new ValidationExpression(Condition.OR, expressionLst, new ValidationField("website"),
                new ValidationField("email"));
        validateJsonMapping(exp);
    }

    private ValidationExpression getCompanyCountryExpression() {
        ValidationExpression innerExp2 = new ValidationExpression(Condition.AND, new ValidationField("company name"),
                new ValidationField("country"));
        return innerExp2;
    }

    private ValidationExpression getCompanyStateExpression() {
        ValidationExpression innerExp1 = new ValidationExpression(Condition.AND, new ValidationField("company name"),
                new ValidationField("state"));
        return innerExp1;
    }

    private void validateJsonMapping(ValidationExpression sourceExp) throws IOException {
        // Convert to JSON
        String json = OM.writeValueAsString(sourceExp);
        log.info("Serialized Expression: \t" + json);
        Assert.assertNotNull(json);
        // Convert to Java Object
        ValidationExpression convertedExp = OM.readValue(json, ValidationExpression.class);
        Assert.assertNotNull(json);
        log.info("Deserialized Expression:" + OM.writeValueAsString(convertedExp));
        Assert.assertTrue(compareValidationExpression(sourceExp, convertedExp));
    }

    private boolean compareValidationExpression(ValidationExpression actual, ValidationExpression expected) {
        // If the object is compared with itself then return true
        if (actual == expected) {
            return true;
        }

        if (expected == null) {
            return false;
        }

        if (!actual.getCondition().equals(expected.getCondition())) {
            return false;
        }
        if (!compareCollections(actual.getExpressions(), expected.getExpressions())) {
            return false;
        }
        if (!compareCollections(actual.getFields(), expected.getFields())) {
            return false;
        }
        return true;
    }

    private boolean compareValidationField(ValidationField actual, ValidationField expected) {
        if (actual == expected) {
            return true;
        }

        if (expected == null) {
            return false;
        }

        if (!actual.getFieldName().equals(expected.getFieldName())) {
            return false;
        }
        return true;
    }

    private boolean compareCollections(List<? extends Object> actual, List<? extends Object> expected) {
        if (CollectionUtils.isEmpty(actual)) {
            if (CollectionUtils.isNotEmpty(expected)) {
                return false;
            }
        } else if (CollectionUtils.isEmpty(expected)) {
            if (CollectionUtils.isNotEmpty(actual)) {
                return false;
            }
        } else if (actual.size() != expected.size()) {
            return false;
        } else {
            for (int i = 0; i < actual.size(); i++) {
                boolean result = false;
                if (actual.get(i) instanceof ValidationExpression) {
                    result = compareValidationExpression((ValidationExpression) actual.get(i),
                            (ValidationExpression) expected.get(i));
                } else if (actual.get(i) instanceof ValidationField) {
                    result = compareValidationField((ValidationField) actual.get(i), (ValidationField) expected.get(i));
                }
                if (!result) {
                    return false;
                }
            }
        }
        return true;
    }

}
