package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.pls.service.LeadEnrichmentService;

@Component("leadEnrichmentService")
public class LeadEnrichmentServiceImpl implements LeadEnrichmentService {

    @Override
    public List<LeadEnrichmentAttribute> getAvariableAttributes() {
        List<LeadEnrichmentAttribute> attributes = new ArrayList<LeadEnrichmentAttribute>();
        for (int i = 0; i < 25; i++) {
            LeadEnrichmentAttribute attr = new LeadEnrichmentAttribute();
            attr.setDisplayName("Attribute " + i);
            attr.setFieldName("Field " + i);
            attr.setFieldType("Test (100)");
            attr.setDescription("Description for attribute " + i);
            attributes.add(attr);
        }
        return attributes;
    }

    @Override
    public List<LeadEnrichmentAttribute> getSavedAttributes() {
        List<LeadEnrichmentAttribute> attributes = new ArrayList<LeadEnrichmentAttribute>();
        for (int i = 0; i < 9; i++) {
            LeadEnrichmentAttribute attr = new LeadEnrichmentAttribute();
            attr.setDisplayName("Attribute " + i);
            attr.setFieldName("Field " + i);
            attr.setFieldType("Test (100)");
            attr.setDescription("Description for attribute " + i);
            attributes.add(attr);
        }
        return attributes;
    }

}
