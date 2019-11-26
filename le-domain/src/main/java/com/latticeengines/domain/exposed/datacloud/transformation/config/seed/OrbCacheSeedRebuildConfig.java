package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrbCacheSeedRebuildConfig extends TransformerConfig {
    @JsonProperty("CompanyFileOrbNumField")
    private String companyFileOrbNumField;

    @JsonProperty("CompanyFileEntityTypeField")
    private String companyFileEntityTypeField;

    @JsonProperty("CompanyFileDomainField")
    private String companyFileDomainField;

    @JsonProperty("CompanyFileWebDomainsField")
    private String companyFileWebDomainsField;

    @JsonProperty("DomainFileOrbNumField")
    private String domainFileOrbNumField;

    @JsonProperty("DomainFileDomainField")
    private String domainFileDomainField;

    @JsonProperty("DomainFileHasEmailField")
    private String domainFileHasEmailField;

    @JsonProperty("OrbCacheSeedDomainField")
    private String orbCacheSeedDomainField;

    @JsonProperty("OrbCacheSeedPrimaryDomainField")
    private String orbCacheSeedPrimaryDomainField;

    @JsonProperty("OrbCacheSeedIsSecondaryDomainField")
    private String orbCacheSeedIsSecondaryDomainField;

    @JsonProperty("OrbCacheSeedDomainHasEmailField")
    private String orbCacheSeedDomainHasEmailField;

    public String getCompanyFileOrbNumField() {
        return companyFileOrbNumField;
    }

    public void setCompanyFileOrbNumField(String companyFileOrbNumField) {
        this.companyFileOrbNumField = companyFileOrbNumField;
    }

    public String getCompanyFileEntityTypeField() {
        return companyFileEntityTypeField;
    }

    public void setCompanyFileEntityTypeField(String companyFileEntityTypeField) {
        this.companyFileEntityTypeField = companyFileEntityTypeField;
    }

    public String getCompanyFileDomainField() {
        return companyFileDomainField;
    }

    public void setCompanyFileDomainField(String companyFileDomainField) {
        this.companyFileDomainField = companyFileDomainField;
    }

    public String getCompanyFileWebDomainsField() {
        return companyFileWebDomainsField;
    }

    public void setCompanyFileWebDomainsField(String companyFileWebDomainsField) {
        this.companyFileWebDomainsField = companyFileWebDomainsField;
    }

    public String getDomainFileOrbNumField() {
        return domainFileOrbNumField;
    }

    public void setDomainFileOrbNumField(String domainFileOrbNumField) {
        this.domainFileOrbNumField = domainFileOrbNumField;
    }

    public String getDomainFileDomainField() {
        return domainFileDomainField;
    }

    public void setDomainFileDomainField(String domainFileDomainField) {
        this.domainFileDomainField = domainFileDomainField;
    }

    public String getDomainFileHasEmailField() {
        return domainFileHasEmailField;
    }

    public void setDomainFileHasEmailField(String domainFileHasEmailField) {
        this.domainFileHasEmailField = domainFileHasEmailField;
    }

    public String getOrbCacheSeedDomainField() {
        return orbCacheSeedDomainField;
    }

    public void setOrbCacheSeedDomainField(String orbCacheSeedDomainField) {
        this.orbCacheSeedDomainField = orbCacheSeedDomainField;
    }

    public String getOrbCacheSeedPrimaryDomainField() {
        return orbCacheSeedPrimaryDomainField;
    }

    public void setOrbCacheSeedPrimaryDomainField(String orbCacheSeedPrimaryDomainField) {
        this.orbCacheSeedPrimaryDomainField = orbCacheSeedPrimaryDomainField;
    }

    public String getOrbCacheSeedIsSecondaryDomainField() {
        return orbCacheSeedIsSecondaryDomainField;
    }

    public void setOrbCacheSeedIsSecondaryDomainField(String orbCacheSeedIsSecondaryDomainField) {
        this.orbCacheSeedIsSecondaryDomainField = orbCacheSeedIsSecondaryDomainField;
    }

    public String getOrbCacheSeedDomainHasEmailField() {
        return orbCacheSeedDomainHasEmailField;
    }

    public void setOrbCacheSeedDomainHasEmailField(String orbCacheSeedDomainHasEmailField) {
        this.orbCacheSeedDomainHasEmailField = orbCacheSeedDomainHasEmailField;
    }

}
