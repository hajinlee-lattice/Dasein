package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.etl.transformation.service.ExternalEnrichService;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

@Component("domainToNameEnrichService")
public class DomainToNameEnrichServiceImpl extends AbstractExternalEnrichService implements ExternalEnrichService {

    private static final Logger log = LoggerFactory.getLogger(DomainToNameEnrichServiceImpl.class);

    protected Set<MatchKey> requiredInputKeys() {
        return Collections.singleton(MatchKey.Domain);
    }

    protected void enrichAndWriteTo(String outputDir, Schema outputSchema, String inputAvroGlob,
                                    Map<MatchKey, List<String>> inputKeyMap) {
        List<GenericRecord> outputRecords = new ArrayList<>();
        List<String> domainFields = inputKeyMap.get(MatchKey.Domain);

        Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, inputAvroGlob);
        while (iterator.hasNext()) {
            GenericRecord record = iterator.next();
            log.info("Enriching record: " + record);
            GenericRecord outputRecord = enrichOneRecord(record, outputSchema, domainFields);
            outputRecords.add(outputRecord);
        }

        outputDir = outputDir.endsWith("/") ? outputDir.substring(0, outputDir.length() - 1) : outputDir;
        String outputFile = outputDir + "/part-0000.avro";

        try {
            if (HdfsUtils.fileExists(yarnConfiguration, outputDir)) {
                HdfsUtils.rmdir(yarnConfiguration, outputDir);
            }
            HdfsUtils.mkdir(yarnConfiguration, outputDir);
            AvroUtils.writeToHdfsFile(yarnConfiguration, outputSchema, outputFile, outputRecords);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write output to target avro", e);
        }

        Long count = AvroUtils.count(yarnConfiguration, outputFile);
        log.info(String.format("Wrote %d records to output file %s.", count, outputFile));
    }

    private GenericRecord enrichOneRecord(GenericRecord inputRecord, Schema outputSchema,
                                          List<String> domainFields) {
        GenericRecordBuilder builder = new GenericRecordBuilder(outputSchema);
        String rawDomain = null;
        for (String domainField: domainFields) {
            Object candidateObj = inputRecord.get(domainField);
            if (candidateObj == null) {
                continue;
            } else if (candidateObj instanceof Utf8) {
                rawDomain = candidateObj.toString();
            } else {
                rawDomain = String.valueOf(candidateObj);
            }
            if (StringUtils.isNotEmpty(rawDomain)) {
                break;
            }
        }
        String domain = DomainUtils.parseDomain(rawDomain);
        log.info("Parsed domain is [" + domain + "]");

        for (Schema.Field outputField: outputSchema.getFields()) {
            String fieldName = outputField.name();
            builder.set(fieldName, null);
        }

        for (Schema.Field inputField: inputRecord.getSchema().getFields()) {
            String fieldName = inputField.name();
            Object value = inputRecord.get(fieldName);
            builder.set(fieldName, value);
        }

        builder.set("Domain", domain);
        builder.set("DUNS", null);
        builder.set("Name", inputRecord.get("Domain"));

        return builder.build();
    }

}
