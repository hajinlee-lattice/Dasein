package com.latticeengines.domain.exposed.camille;

public class Service {
    private String serviceName;
    private int dataVersion;
    
    public Service(String serviceName, int dataVersion) {
        this.setServiceName(serviceName);
        this.setDataVersion(dataVersion);
    }

    public int getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(int dataVersion) {
        this.dataVersion = dataVersion;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
