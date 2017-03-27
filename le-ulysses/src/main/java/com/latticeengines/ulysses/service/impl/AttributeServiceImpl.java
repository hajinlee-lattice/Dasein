package com.latticeengines.ulysses.service.impl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.domain.exposed.ulysses.PrimaryField;
import com.latticeengines.domain.exposed.ulysses.PrimaryFieldValidationExpression;
import com.latticeengines.ulysses.service.AttributeService;

@Component("attributeService")
public class AttributeServiceImpl implements AttributeService {

    private static final Logger log = Logger.getLogger(AttributeServiceImpl.class);

    @Override
    public List<PrimaryField> getPrimaryFields() {
        EnumSet<FieldInterpretation> primaryMatchingFields = FieldInterpretationCollections.PrimaryMatchingFields;
        List<PrimaryField> primaryFields = new ArrayList<>();
        for (FieldInterpretation pmf : primaryMatchingFields) {
            primaryFields.add(new PrimaryField(pmf.getFieldName(), pmf.getFieldType().name(), pmf.getDisplayName()));
        }
        return primaryFields;
    }

    @Override
    public PrimaryFieldValidationExpression getPrimaryFieldValidationExpression(CustomerSpace customerSpace) {
        boolean fuzzyMatchEnabled = FeatureFlagClient.isEnabled(customerSpace,
                LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName());
        log.debug("Fuzzy Matching Enabled for client: " + fuzzyMatchEnabled);
        String expression = fuzzyMatchEnabled ? FieldInterpretationCollections.FUZZY_MATCH_VALIDATION_EXPRESSION
                : FieldInterpretationCollections.NON_FUZZY_MATCH_VALIDATION_EXPRESSION;

        PrimaryFieldValidationExpression validationExp = new PrimaryFieldValidationExpression();
        validationExp.setExpression(expression);
        return validationExp;
    }

}
