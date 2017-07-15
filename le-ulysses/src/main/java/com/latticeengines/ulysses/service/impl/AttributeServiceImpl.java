package com.latticeengines.ulysses.service.impl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.domain.exposed.ulysses.PrimaryField;
import com.latticeengines.domain.exposed.ulysses.PrimaryFieldValidationExpression;
import com.latticeengines.ulysses.service.AttributeService;

@Component("attributeService")
public class AttributeServiceImpl implements AttributeService {

    private static final Logger log = LoggerFactory.getLogger(AttributeServiceImpl.class);

    @Autowired
    private BatonService batonService;

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
        boolean fuzzyMatchEnabled = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_FUZZY_MATCH);
        log.debug("Fuzzy Matching Enabled for client: " + fuzzyMatchEnabled);
        String expression = fuzzyMatchEnabled ? FieldInterpretationCollections.FUZZY_MATCH_VALIDATION_EXPRESSION
                : FieldInterpretationCollections.NON_FUZZY_MATCH_VALIDATION_EXPRESSION;
        PrimaryFieldValidationExpression validationExp = new PrimaryFieldValidationExpression();
        validationExp.setExpression(expression);
        return validationExp;
    }

}
