package com.latticeengines.propdata.madison.service;

import java.util.List;

public interface PropDataMadisonDataFlowService {

    void execute(String flowName, List<String> sourcePaths, String targetPath, String targetSchemaPath);
}
