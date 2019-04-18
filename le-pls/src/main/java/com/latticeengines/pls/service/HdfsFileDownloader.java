package com.latticeengines.pls.service;


public interface HdfsFileDownloader {

    String getFileContents(String tenantId, String model, String filter) throws Exception;

}
