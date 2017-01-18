package com.latticeengines.domain.exposed.attribute;

import jdk.nashorn.internal.ir.annotations.Ignore;

import org.apache.avro.reflect.AvroIgnore;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.CompositeFabricEntity;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

public class AttributeCustomization extends CompositeFabricEntity implements HasName, HasTenant {
    @JsonProperty("attribute_name")
    @DynamoAttribute("attribute_name")
    private String attributeName;

    @JsonProperty("company_profile_flags")
    @DynamoAttribute("company_profile_flags")
    private CompanyProfileAttributeFlags companyProfileFlags;

    @Ignore
    @AvroIgnore
    private Tenant tenant;

    @Override
    public String getName() {
        return attributeName;
    }

    @Override
    public void setName(String name) {
        setEntityId(name);
        this.attributeName = name;
    }

    public CompanyProfileAttributeFlags getCompanyProfileFlags() {
        return companyProfileFlags;
    }

    public void setCompanyProfileFlags(CompanyProfileAttributeFlags companyProfileFlags) {
        this.companyProfileFlags = companyProfileFlags;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setId(tenant.getId(), "AttributeCustomization", getEntityId());
        }
    }
}
