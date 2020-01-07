package com.latticeengines.pls.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;

public interface ImportWorkflowService {
    ImportWorkflowSpec validateIndividualSpec(String systemType, String systemObject, InputStream specInputStream,
                                              List<String> errors) throws Exception;

    String uploadIndividualSpec(String systemType, String systemObject, InputStream specInputStream) throws Exception;

    void downloadSpecFromS3(HttpServletRequest request, HttpServletResponse response, String mimeType, String systemType,
                            String systemObject) throws IOException;

    List<ImportWorkflowSpec> getSpecsByTypeAndObject(String customerSpace, String systemType,
                                                     String systemObject);
}
