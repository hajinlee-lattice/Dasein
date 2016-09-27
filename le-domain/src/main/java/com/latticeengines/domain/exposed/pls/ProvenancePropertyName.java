package com.latticeengines.domain.exposed.pls;

public enum ProvenancePropertyName {
    IsOneLeadPerDomain("Is_One_Lead_Per_Domain", Boolean.class), //
    ExcludePropdataColumns("Exclude_Propdata_Columns", Boolean.class), //
    ExcludePublicDomains("Exclude_Public_Domains", Boolean.class),
    TrainingFilePath("Training_File_Path", String.class);

    private String name;
    private Class<?> type;

    ProvenancePropertyName(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public Class<?> getType() {
        return this.type;
    }
}
