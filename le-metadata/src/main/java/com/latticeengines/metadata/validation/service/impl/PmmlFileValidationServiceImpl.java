package com.latticeengines.metadata.validation.service.impl;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.pmml.PmmlField;
import com.latticeengines.domain.exposed.util.PmmlModelUtils;

@Component("pmmlFileValidationService")
public class PmmlFileValidationServiceImpl extends ArtifactValidation {

    protected PmmlFileValidationServiceImpl() {
        super(ArtifactType.PMML);
    }

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public String validate(String filePath) {
        String messg = "";
        try {
            if (StringUtils.isEmpty(filePath) || !HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                return new LedpException(LedpCode.LEDP_10011, new String[] { new Path(filePath).getName() })
                        .getMessage();
            }
            try (InputStream pmmlStream = HdfsUtils.getInputStream(yarnConfiguration, filePath)) {
                List<PmmlField> pmmlFields = PmmlModelUtils.getPmmlFields(pmmlStream);
                messg = validatePmmlField(pmmlFields);
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            return LedpCode.LEDP_10008.getMessage();
        }
        return messg;
    }

    private String validatePmmlField(List<PmmlField> pmmlFields) {
        StringBuilder sb = new StringBuilder();
        for (PmmlField pmmlField : pmmlFields) {
            if (!AvroUtils.isAvroFriendlyFieldName(pmmlField.miningField.getName().getValue())) {
                sb.append("MiningField: ").append(pmmlField.miningField.getName()).append(" has invalid value.\n");
            }
            if (!AvroUtils.isAvroFriendlyFieldName(pmmlField.miningField.getName().getValue())) {
                sb.append("DataField: ").append(pmmlField.miningField.getName()).append(" has invalid value.\n");
            }
        }
        return sb.toString();
    }
}
