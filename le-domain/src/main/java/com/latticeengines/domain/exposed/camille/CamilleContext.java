package com.latticeengines.domain.exposed.camille;

public class CamilleContext {
    private CustomerSpace customerSpace;
    private String service;
    private int dataVersion;
    
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }
    
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }
    
    public String getService() {
        return service;
    }
    
    public void setService(String service) {
        this.service = service;
    }
    
    public int getDataVersion() {
        return dataVersion;
    }
    
    public void setDataVersion(int dataVersion) {
        this.dataVersion = dataVersion;
    }
}
