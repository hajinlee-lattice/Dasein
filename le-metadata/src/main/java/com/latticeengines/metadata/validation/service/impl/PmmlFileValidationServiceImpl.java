package com.latticeengines.metadata.validation.service.impl;

import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.dmg.pmml.PMML;
import org.jpmml.schema.Version;
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

    public static final Set<String> SUPPORTED_VERSIONS = new TreeSet<>();

    static {
        for (Version version : Version.values()) {
            if (!version.getVersion().equals("3.0")) {
                SUPPORTED_VERSIONS.add(version.getVersion());
            }
        }
    }

    protected PmmlFileValidationServiceImpl() {
        super(ArtifactType.PMML);
    }

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public void validate(String filePath) {
        try {
            if (StringUtils.isEmpty(filePath) || !HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                throw new LedpException(LedpCode.LEDP_10011, new String[] { new Path(filePath).getName() });
            }
            try (InputStream pmmlStream = HdfsUtils.getInputStream(yarnConfiguration, filePath)) {
                PMML pmml = PmmlModelUtils.getPMMLWithOriginalVersion(pmmlStream);
                validatePmmlVersion(pmml);
                List<PmmlField> pmmlFields = PmmlModelUtils.getPmmlFields(pmml);
                validatePmmlField(pmmlFields);
            }
        } catch (LedpException le) {
            throw new RuntimeException(le);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_10008);
        }
    }

    private void validatePmmlField(List<PmmlField> pmmlFields) {
        StringBuilder sb = new StringBuilder();
        for (PmmlField pmmlField : pmmlFields) {
            if (!AvroUtils.isAvroFriendlyFieldName(pmmlField.miningField.getName().getValue())) {
                sb.append("MiningField: ").append(pmmlField.miningField.getName()).append(" has invalid value.\n");
            }
            if (!AvroUtils.isAvroFriendlyFieldName(pmmlField.miningField.getName().getValue())) {
                sb.append("DataField: ").append(pmmlField.miningField.getName()).append(" has invalid value.\n");
            }
        }
        if (sb.length() > 0) {
            throw new LedpException(LedpCode.LEDP_18115, new String[] { sb.toString() });
        }
    }

    private void validatePmmlVersion(PMML pmml) {
        String version = pmml.getVersion();
        if (!SUPPORTED_VERSIONS.contains(version)) {
            throw new LedpException(LedpCode.LEDP_28028, new String[] { version, SUPPORTED_VERSIONS.toString() });
        }
    }
}
