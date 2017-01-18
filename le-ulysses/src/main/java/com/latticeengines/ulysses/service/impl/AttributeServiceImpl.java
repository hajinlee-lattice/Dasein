package com.latticeengines.ulysses.service.impl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.attribute.PrimaryField;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.domain.exposed.validation.Condition;
import com.latticeengines.domain.exposed.validation.ValidationExpression;
import com.latticeengines.domain.exposed.validation.ValidationField;
import com.latticeengines.ulysses.service.AttributeService;

@Component("attributeService")
public class AttributeServiceImpl implements AttributeService {

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
    public ValidationExpression getPrimaryFieldValidationExpression() {

        // Website OR Email OR CompanyName
        ValidationExpression primaryMatchFields = new ValidationExpression(Condition.OR,
                new ValidationField(FieldInterpretation.Website.getFieldName()),
                new ValidationField(FieldInterpretation.Email.getFieldName()),
                new ValidationField(FieldInterpretation.CompanyName.getFieldName()));

        // primaryMatchFields AND Id
        // ID field is discussed at:
        // https://solutions.lattice-engines.com/browse/PLS-2860
        ValidationExpression finalExpression = new ValidationExpression(Condition.AND, primaryMatchFields,
                new ValidationField(FieldInterpretation.Id.getFieldName()));

        return finalExpression;
    }

}
