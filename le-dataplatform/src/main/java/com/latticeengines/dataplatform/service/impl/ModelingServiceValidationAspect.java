package com.latticeengines.dataplatform.service.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

@Aspect
public class ModelingServiceValidationAspect {

    @Inject
    private BeanValidationService beanValidationService;

    @Inject
    private DbMetadataService dbMetadataService;

    @Before("execution(* com.latticeengines.dataplatform.service.impl.ModelingServiceImpl.loadData(..)) "
            + " && args(config)")
    public void validateLoad(LoadConfiguration config) {
        validateLoadConfig(config);
        validateEventTableColumnNames(config);
    }

    @Before("execution(* com.latticeengines.dataplatform.service.impl.ModelingServiceImpl.createSamples(..)) "
            + " && args(config)")
    public void validateCreateSamples(SamplingConfiguration config) {
        validateCreateSamplesConfig(config);
    }

    @Before("execution(* com.latticeengines.dataplatform.service.impl.ModelingServiceImpl.profileData(..)) "
            + " && args(config)")
    public void validateProfileData(DataProfileConfiguration config) {
        validateProfileDataConfig(config);
    }

    @Before("execution(* com.latticeengines.dataplatform.service.impl.ModelingServiceImpl.submitModel(..)) "
            + " && args(model)")
    public void validateSubmitMode(Model model) {
        validateSubmitModelConfig(model);
    }

    void validateLoadConfig(LoadConfiguration config) {
        if (!isElementValidInPath(config.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getCustomer() });
        }
        if (!isElementValidInPath(config.getTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getTable() });
        }
        if (!isElementValidInPath(config.getMetadataTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getMetadataTable() });
        }
    }

    void validateEventTableColumnNames(LoadConfiguration config) {
        JdbcTemplate jdbcTemplate = dbMetadataService.constructJdbcTemplate(config.getCreds());
        List<String> columnNames = dbMetadataService.getColumnNames(jdbcTemplate, config.getTable());
        for (String columnName : columnNames) {
            validateColumnName(columnName);
        }
    }

    void validateColumnName(String columnName) {
        String invalidChars = " :/";
        if (StringUtils.containsAny(columnName, invalidChars))
            throw new LedpException(LedpCode.LEDP_10007, new String[] { columnName
                    + " which contains invalid characters." });

    }

    void validateCreateSamplesConfig(SamplingConfiguration config) {
        if (!isElementValidInPath(config.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getCustomer() });
        }
        if (!isElementValidInPath(config.getTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getTable() });
        }
        Set<AnnotationValidationError> validationErrors;
        try {
            validationErrors = beanValidationService.validate(config);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15013);
        }
        if (validationErrors.size() != 0) {
            throw new LedpException(LedpCode.LEDP_15012);
        }
    }

    void validateProfileDataConfig(DataProfileConfiguration config) {
        if (!isElementValidInPath(config.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getCustomer() });
        }
        if (!isElementValidInPath(config.getTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getTable() });
        }
    }

    void validateSubmitModelConfig(Model config) {
        if (!isElementValidInPath(config.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getCustomer() });
        }
        if (!isElementValidInPath(config.getTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getTable() });
        }
    }

    private boolean isElementValidInPath(String value) {

        if (StringUtils.containsAny(value, "{}[]/:")) {
            return false;
        }
        String[] invalidList = { "", ".", "..", "/" };
        Set<String> invalidElements = new HashSet<>(Arrays.asList(invalidList));
        if (invalidElements.contains(value)) {
            return false;
        }

        return true;
    }
}
