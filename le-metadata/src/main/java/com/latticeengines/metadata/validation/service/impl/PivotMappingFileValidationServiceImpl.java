package com.latticeengines.metadata.validation.service.impl;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ArtifactType;

@Component("pivotMappingFileValidationService")
public class PivotMappingFileValidationServiceImpl extends ArtifactValidation {

    @Autowired
    private Configuration yarnConfiguration;

    private String[] expectedHeaders = new String[] { "SourceColumn", "TargetColumn", "SourceColumnType", "Value" };

    protected PivotMappingFileValidationServiceImpl() {
        super(ArtifactType.PivotMapping);
    }

    @Override
    public void validate(String filePath) {
        try {
            if (StringUtils.isEmpty(filePath) || !HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                throw new LedpException(LedpCode.LEDP_10011, new String[] { new Path(filePath).getName() });
            }
            try (Reader reader = new InputStreamReader(new BOMInputStream(HdfsUtils.getInputStream(yarnConfiguration, //
                    filePath)), "UTF-8")) {
                try (CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader())) {
                    validateHeader(parser.getHeaderMap().keySet());
                    validateSourceColumn(parser.iterator());
                }
            }
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_10008);
        }
    }

    private void validateHeader(Set<String> headers) {
        List<String> missingHeaders = new ArrayList<>();
        for (String expectedHeader : expectedHeaders) {
            if (!headers.contains(expectedHeader)) {
                missingHeaders.add(expectedHeader);
            }
        }
        if (!CollectionUtils.isEmpty(missingHeaders)) {
            throw new LedpException(LedpCode.LEDP_10009, new String[] { missingHeaders.toString() });
        }

    }

    private void validateSourceColumn(Iterator<CSVRecord> iter) {
        while (iter.hasNext()) {
            String sourceColumn = iter.next().get("SourceColumn");
            if (!NameValidationUtils.validateColumnName(sourceColumn)) {
                throw new LedpException(LedpCode.LEDP_10012, new String[] { sourceColumn });
            }
        }

    }
}
