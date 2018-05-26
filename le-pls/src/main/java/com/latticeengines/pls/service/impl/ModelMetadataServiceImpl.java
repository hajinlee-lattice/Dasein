package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.proxy.exposed.lp.ModelMetadataProxy;

@Component("modelMetadataService")
public class ModelMetadataServiceImpl implements ModelMetadataService {

    @Inject
    private ModelMetadataProxy modelMetadataProxy;

    @Override
    public Table getEventTableFromModelId(String modelId) {
        return modelMetadataProxy.getEventTableFromModelId(MultiTenantContext.getTenantId(), modelId);
    }

    @Override
    public List<VdbMetadataField> getMetadata(String modelId) {
        return modelMetadataProxy.getMetadata(MultiTenantContext.getTenantId(), modelId);
    }

    @Override
    public List<String> getRequiredColumnDisplayNames(String modelId) {
        return modelMetadataProxy.getRequiredColumnDisplayNames(MultiTenantContext.getTenantId(), modelId);
    }

    @Override
    public List<Attribute> getRequiredColumns(String modelId) {
        return modelMetadataProxy.getRequiredColumns(MultiTenantContext.getTenantId(), modelId);
    }

    @Override
    public Set<String> getLatticeAttributeNames(String modelId) {
        return modelMetadataProxy.getLatticeAttributeNames(MultiTenantContext.getTenantId(), modelId);
    }

    @Override
    public List<Attribute> getAttributesFromFields(List<Attribute> attributes, List<VdbMetadataField> fields) {
        return modelMetadataProxy.getAttributesFromFields(MultiTenantContext.getTenantId(), attributes, fields);
    }

}
