package com.latticeengines.domain.exposed.pls;

public enum DataLicense {

    HG("HG", "technology"), BOMBORA("Bombora", "Intent"), WEBSITEKEYWORDS("WebsiteKeywords",
            "Website Keywords"), ACCOUNT("Account", "My attributes"), CONTACT("Contact", "Contact attributes");

    private String dataLicense;
    private String description;

    DataLicense(String dataLicense, String description) {
        this.dataLicense = dataLicense;
        this.description = description;
    }

    public String getDataLicense() {
        return dataLicense;
    }

    public String getDescription() {
        return description;
    }
}
