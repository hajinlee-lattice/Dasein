package com.latticeengines.pls.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

import javax.annotation.Nullable;

import com.latticeengines.common.exposed.csv.LECSVFormat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;

public class ValidateFileHeaderUtils {

    private static final Logger log = Logger.getLogger(ValidateFileHeaderUtils.class);

    public static final int BIT_PER_BYTE = 1024;
    public static final int BYTE_NUM = 500;
    public static final int MAX_NUM_ROWS = 100;

    public static Set<String> getCSVHeaderFields(InputStream stream, CloseableResourcePool closeableResourcePool) {
        try {
            Set<String> headerFields = null;
            InputStreamReader reader = new InputStreamReader(new BOMInputStream(stream, false, ByteOrderMark.UTF_8,
                    ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                    StandardCharsets.UTF_8);
            CSVFormat format = LECSVFormat.format;
            CSVParser parser = new CSVParser(reader, format);
            closeableResourcePool.addCloseable(parser);
            headerFields = parser.getHeaderMap().keySet();
            // make this temporary fix
            if (!parser.iterator().hasNext()) {
                throw new RuntimeException("Expected at least 1 record. Instead found 0");
            }

            return headerFields;

        } catch (IllegalArgumentException e) {
            throw new LedpException(LedpCode.LEDP_18109, new String[] { e.getMessage() });
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    public static List<String> getCSVColumnValues(String columnHeaderName, InputStream stream,
            CloseableResourcePool closeableResourcePool) {
        try {
            List<String> columnFields = new ArrayList<>();
            InputStreamReader reader = new InputStreamReader(new BOMInputStream(stream, false, ByteOrderMark.UTF_8,
                    ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                    StandardCharsets.UTF_8);
            CSVFormat format = LECSVFormat.format;
            CSVParser parser = new CSVParser(reader, format);
            Iterator<CSVRecord> csvRecordIterator = parser.iterator();
            closeableResourcePool.addCloseable(parser);

            if (!parser.iterator().hasNext()) {
                throw new RuntimeException("Expected at least 1 record. Instead found 0");
            }
            int i = 0;
            while (i < MAX_NUM_ROWS && csvRecordIterator.hasNext()) {
                String columnField = csvRecordIterator.next().get(columnHeaderName);
                if (columnField != null && !columnField.isEmpty()) {
                    columnFields.add(columnField);
                }
                i++;
            }

            return columnFields;
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static void checkForDuplicateHeaders(List<Attribute> attributes, String fileDisplayName,
            Set<String> headerFields) {
        Map<String, List<String>> duplicates = new HashMap<>();
        for (final Attribute attribute : attributes) {
            final List<String> allowedDisplayNames = attribute.getAllowedDisplayNames();
            if (allowedDisplayNames != null) {
                Iterable<String> filtered = Iterables.filter(headerFields, new Predicate<String>() {
                    @Override
                    public boolean apply(@Nullable String input) {
                        return allowedDisplayNames.contains(input.toUpperCase())
                                || (input != null && input.equalsIgnoreCase(attribute.getDisplayName()));
                    }
                });

                if (Iterables.size(filtered) > 1) {
                    duplicates.put(attribute.getName(), Lists.newArrayList(filtered));
                }
            }
        }

        if (duplicates.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, List<String>> entry : duplicates.entrySet()) {
                sb.append(String
                        .format("In file %s, cannot have columns %s as CSV headers because they correspond to the same information (%s)\n",
                                fileDisplayName, StringUtils.join(entry.getValue()), entry.getKey()));
            }
            throw new LedpException(LedpCode.LEDP_18107, new String[] { sb.toString() });
        }
    }

    @SuppressWarnings("unchecked")
    public static void checkForEmptyHeaders(String fileDisplayName, Set<String> headerFields) {
        for (final String field : headerFields) {
            if (StringUtils.isEmpty(field)) {
                throw new LedpException(LedpCode.LEDP_18096, new String[] { fileDisplayName });
            }
        }
    }

    // UNUSED
    public static void checkForMissingRequiredFields(List<Attribute> attributes, String fileDisplayName,
            Set<String> headerFields, boolean respectNullability) {

        Set<String> missingRequiredFields = new HashSet<>();
        Iterator<Attribute> attrIterator = attributes.iterator();

        iterateAttr: while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            Iterator<String> headerIterator = headerFields.iterator();

            while (headerIterator.hasNext()) {
                String header = headerIterator.next();
                if (attribute.getAllowedDisplayNames() != null
                        && attribute.getAllowedDisplayNames().contains(header.toUpperCase())) {
                    continue iterateAttr;
                } else if (attribute.getDisplayName().equalsIgnoreCase(header)) {
                    continue iterateAttr;
                }
            }
            // didn't find the attribute
            if (!respectNullability) {
                missingRequiredFields.add(attribute.getName());
            } else if (!attribute.isNullable()) {
                missingRequiredFields.add(attribute.getName());
            }
        }
        if (!missingRequiredFields.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_18087, //
                    new String[] { StringUtils.join(missingRequiredFields, ","), fileDisplayName });
        }

        checkForEmptyHeaders(fileDisplayName, headerFields);
    }

    public static void checkForHeaderFormat(Set<String> headerFields) {
        if (headerFields.size() == 1) {
            throw new LedpException(LedpCode.LEDP_19111);
        }
    }
}
