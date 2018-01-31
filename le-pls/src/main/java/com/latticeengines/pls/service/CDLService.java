package com.latticeengines.pls.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public interface CDLService {

    ApplicationId processAnalyze(String string);

    ApplicationId submitCSVImport(String customerSpace, String templateFileName, String dataFileName, String source,
                                  String entity, String feedType);

    ApplicationId cleanup(String customerSpace, String sourceFileName, SchemaInterpretation schemaInterpretation,
                          CleanupOperationType cleanupOperationType);

}
