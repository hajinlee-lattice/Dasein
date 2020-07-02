package com.latticeengines.domain.exposed.metadata.standardschemas;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ImportWorkflowSpec extends FieldDefinitionsRecord {

    @JsonProperty(required = false)
    protected SectionConfiguration sectionConfiguration;

    public SectionConfiguration getSectionConfiguration() {
        return sectionConfiguration;
    }

    public void setSectionConfiguration(SectionConfiguration sectionConfiguration) {
        this.sectionConfiguration = sectionConfiguration;
    }

    // Set default values of section configuration fields if they were not included in the Spec when imported
    // from S3.
    public void setDefaultSectionConfiguration() {
        if (sectionConfiguration == null) {
            sectionConfiguration = new SectionConfiguration();
        }
        if (StringUtils.isBlank(sectionConfiguration.customFieldsSectionName)) {
            sectionConfiguration.setCustomFieldsSectionName(FieldDefinitionSectionName.Custom_Fields.getName());
        }
    }

    public String provideCustomFieldsSectionName() {
        // Defensively code to set up the SectionConfiguration object for legacy Specs.
        setDefaultSectionConfiguration();
        return sectionConfiguration.getCustomFieldsSectionName();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof ImportWorkflowSpec) {
            ImportWorkflowSpec spec = (ImportWorkflowSpec) object;
            return super.equals(spec);
        }
        return false;
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public class SectionConfiguration {

        @JsonProperty(required = false)
        protected String customFieldsSectionName;


        public String getCustomFieldsSectionName() {
            return customFieldsSectionName;
        }

        public void setCustomFieldsSectionName(String customFieldsSectionName) {
            this.customFieldsSectionName = customFieldsSectionName;
        }
    }
}


