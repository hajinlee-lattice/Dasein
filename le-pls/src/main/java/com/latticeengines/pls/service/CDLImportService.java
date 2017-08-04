package com.latticeengines.pls.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface CDLImportService {

    ApplicationId submitCSVImport(String customerSpace, String templateFileName, String dataFileName, String source,
                                  String entity, String feedType);
}
