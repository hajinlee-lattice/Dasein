package com.latticeengines.cdl.workflow.steps.validations.service.impl;

import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.validations.service.InputFileValidationService;
import com.latticeengines.common.exposed.validator.URLValidator;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.CatalogFileValidationConfiguration;

@Component("catalogFileValidationService")
@Lazy(value = false)
public class CatalogFileValidationService extends InputFileValidationService<CatalogFileValidationConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CatalogFileValidationService.class);

    @Override
    public long validate(CatalogFileValidationConfiguration catalogFileValidationServiceConfiguration,
                         List<String> processedRecords, StringBuilder statistics) {
        List<String> pathList = catalogFileValidationServiceConfiguration.getPathList();
        // copy error file if file exists
        String errorFile = getPath(pathList.get(0)) + PATH_SEPARATOR + ImportProperty.ERROR_FILE;
        CSVFormat format = copyErrorFileToLocalIfExist(errorFile);
        URLValidator urlValidator = new URLValidator();
        InterfaceName pathPattern = InterfaceName.PathPattern;

        long errorLine = handleRecords(format, pathList, processedRecords, pathPattern, urlValidator);
        // copy error file back to hdfs if needed, remove temporary error.csv generated in local
        if (errorLine != 0L) {
            copyErrorFileBackToHdfs(errorFile);
        }
        return errorLine;
    }
}
