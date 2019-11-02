package com.latticeengines.domain.exposed.pls.frontend;

public enum FieldDefinitionSectionName {

    Unique_ID("Unique ID"),
    Match_To_Accounts_ID("Match to Accounts - ID"),
    Custom_Fields("Custom Fields"),
    Other_IDs("Other IDs"),
    Analysis_Fields("Analysis Fields"),
    Match_To_Accounts_Fields("Match to Accounts - Fields"),
    Match_IDs("Match IDs"),
    Contact_Fields("Contact Fields");

    FieldDefinitionSectionName(String name) {
        this.name = name;
    }

    private String name;

    public String getName() {
        return name;
    }

}
