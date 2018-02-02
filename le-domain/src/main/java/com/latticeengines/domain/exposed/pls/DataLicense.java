package com.latticeengines.domain.exposed.pls;

public enum DataLicense {

    HG("HG"), BOMBORA("Bombora");

    private String dataLicense;

    private DataLicense(String dataLicense) {
        this.dataLicense = dataLicense;
    }

    public String getDataLicense() {
        return dataLicense;
    }

}
