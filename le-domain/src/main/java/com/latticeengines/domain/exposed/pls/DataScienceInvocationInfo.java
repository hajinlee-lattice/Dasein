package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataScienceInvocationInfo {

    @JsonProperty("user_id")
    private String userId;

    // Store for underlying data files.
    // Defaults to HDFS, but may be used
    // for pure file operations in test.
    @JsonProperty("storage_protocol")
    private String storageProtocol;

    // Path to the directory housing individual models
    // and ancillary files
    @JsonProperty("modelDirectory")
    private String modelDirectory;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("customer")
    private String customer;

    @JsonProperty("customerSpace")
    private String customerSpace;

    @JsonProperty("extractsDirectory")
    private String extractsDirectory;

    @JsonProperty("file_type_map")
    private Map<String, String> fileTypeMap;

    @JsonProperty("metadata_file")
    private String metadataFile;

    @JsonProperty("hdfs_web_url")
    private String hdfsWebURL;

    @JsonProperty("model_lookup_id")
    private String modelLookupID;

    @JsonProperty("model_application_id")
    private String modelApplicationID;

    public DataScienceInvocationInfo() {
        storageProtocol = "HDFS";
    }


    public String getCustomerSpace(){return customerSpace;}

    public void setCustomerSpace(String customerSpace){this.customerSpace = customerSpace;}

    public String getCustomer(){return customer;}

    public void setCustomer(String customer){this.customer = customer;}

    public String getStorageProtocol(){return storageProtocol;}

    public void setStorageProtocol(String storageProtocol){this.storageProtocol = storageProtocol;}

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getModelId() { return modelId; }

    public void setModelId(String modelId) {this.modelId = modelId;}

    public String getModelDirectory(){return modelDirectory;}

    public void setModelDirectory(String modelDirectory){this.modelDirectory = modelDirectory;}

    public  String getExtractsDirectory(){return extractsDirectory;}

    public void setExtractsDirectory(String extractsDirectory){this.extractsDirectory = extractsDirectory;}

    public String getMetadataFile(){return metadataFile;}

    public void setMetadataFile(String metadataFile){this.metadataFile = metadataFile;}

    public Map<String, String> getFileTypeMap() {
        return fileTypeMap;
    }

    public void setFileTypeMap(Map<String, String> fileTypeMap) {
        this.fileTypeMap = fileTypeMap;
    }

    public String getHdfsWebURL()
    {
        return hdfsWebURL;
    }

    public void setHdfsWebURL(String hdfsWebURL)
    {
        this.hdfsWebURL = hdfsWebURL;
    }

    public String getModelLookupID()
    {
        return modelLookupID;
    }

    public void setModelLookupID(String modelLookupID)
    {
        this.modelLookupID = modelLookupID;
    }

    public String getModelApplicationID()
    {
        return modelApplicationID;
    }

    public void setModelApplicationID(String modelApplicationID)
    {
        this.modelApplicationID = modelApplicationID;
    }
}

