package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.PrimaryAttributeService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.attribute.PrimaryField;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.domain.exposed.ulysses.PrimaryFieldValidationExpression;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;

@Component("primaryAttributeService")
public class PrimaryAttributeServiceImpl implements PrimaryAttributeService {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    private BatonService batonService;

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Override
    public List<PrimaryField> getPrimaryFields() {
        EnumSet<FieldInterpretation> primaryMatchingFields = FieldInterpretationCollections.PrimaryMatchingFields;
        List<PrimaryField> primaryFields = new ArrayList<>();
        for (FieldInterpretation pmf : primaryMatchingFields) {
            primaryFields.add(new PrimaryField(pmf.getFieldName(), pmf.getFieldType().name(), pmf.getDisplayName()));
        }
        return primaryFields;
    }

    @SuppressWarnings("deprecation")
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

    @Override
    public List<PrimaryField> getPrimaryFieldsFromExternalSystem(CustomerSpace customerSpace, String type) {
        List<CDLExternalSystemMapping> externalSystemMappings =
                cdlExternalSystemProxy.getExternalSystemByType(customerSpace.toString(),
                        CDLExternalSystemType.valueOf(type));
        List<PrimaryField> primaryFields = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(externalSystemMappings)) {
            externalSystemMappings.forEach(externalSystemMapping ->
                    primaryFields.add(new PrimaryField(externalSystemMapping.getFieldName(),
                            externalSystemMapping.getFieldType(), externalSystemMapping.getDisplayName())));
        }
        return primaryFields;
    }

    @Override
    public Map<String, List<PrimaryField>> getPrimaryFieldsFromExternalSystem(CustomerSpace customerSpace) {
        Map<String, List<CDLExternalSystemMapping>> externalSystemMappings =
                cdlExternalSystemProxy.getExternalSystemMap(customerSpace.toString());
        Map<String, List<PrimaryField>> primaryFieldsMap = new HashMap<>();
        if (MapUtils.isNotEmpty(externalSystemMappings)) {
            for (Map.Entry<String, List<CDLExternalSystemMapping>> systemEntry : externalSystemMappings.entrySet()) {
                List<PrimaryField> primaryFields= new ArrayList<>();
                systemEntry.getValue().forEach(system -> primaryFields.add(new PrimaryField(system.getFieldName(),
                        system.getFieldType(), system.getDisplayName())));
                primaryFieldsMap.put(systemEntry.getKey(), primaryFields);
            }
        }
        return primaryFieldsMap;
    }
}
