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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ActivityStreamFileValidationConfiguration;

@Component("activityStreamValidationService")
@Lazy(value = false)
public class ActivityStreamValidationService extends InputFileValidationService<ActivityStreamFileValidationConfiguration> {

    private static Logger log = LoggerFactory.getLogger(ActivityStreamValidationService.class);

    @Override
    public long validate(ActivityStreamFileValidationConfiguration inputFileValidationServiceConfiguration,
                         List<String> processedRecords, StringBuilder statistics) {
        List<String> pathList = inputFileValidationServiceConfiguration.getPathList();
        // copy error file if file exists
        String errorFile = getPath(pathList.get(0)) + PATH_SEPARATOR + ImportProperty.ERROR_FILE;
        CSVFormat format = copyErrorFileToLocalIfExist(errorFile);

        InterfaceName webVisitPageUrl = InterfaceName.WebVisitPageUrl;
        URLValidator urlValidator = new URLValidator();
        long errorLine = handleRecords(format, pathList, processedRecords, webVisitPageUrl, urlValidator);

        // copy error file back to hdfs if needed, remove temporary error.csv generated in local
        if (errorLine != 0L) {
            copyErrorFileBackToHdfs(errorFile);
        }
        return errorLine;
    }
}
