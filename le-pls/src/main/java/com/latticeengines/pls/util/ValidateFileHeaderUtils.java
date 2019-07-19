package com.latticeengines.pls.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.apache.avro.SchemaParseException;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.latticeengines.app.exposed.service.impl.CommonTenantConfigServiceImpl;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;

public class ValidateFileHeaderUtils {

    private static final Logger log = LoggerFactory.getLogger(ValidateFileHeaderUtils.class);

    public static final int BIT_PER_BYTE = 1024;
    public static final int BYTE_NUM = 500;
    public static final int MAX_NUM_ROWS = 100;
    public static final int MAX_HEADER_LENGTH = 63;
    public static final String AVRO_FIELD_NAME_PREFIX = "avro_";
    public static final String CSV_INJECTION_CHARACHTERS = "@+-=";

    @Inject
    private CommonTenantConfigServiceImpl appTenantConfigService;

    public static Set<String> getCSVHeaderFields(InputStream stream, CloseableResourcePool closeableResourcePool) {
        try {
            Set<String> headerFields = null;
            InputStreamReader reader = new InputStreamReader(
                    new BOMInputStream(stream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                            ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                    StandardCharsets.UTF_8);

            CSVFormat format = LECSVFormat.format;
            CSVParser parser = new CSVParser(reader, format);
            closeableResourcePool.addCloseable(parser);
            headerFields = parser.getHeaderMap().keySet();
            // make this temporary fix
            if (!parser.iterator().hasNext()) {
                throw new LedpException(LedpCode.LEDP_18110);
            }

            return headerFields;

        } catch (IllegalArgumentException e) {
            throw new LedpException(LedpCode.LEDP_18109, new String[] { e.getMessage() });
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    public static List<String> getCSVColumnValues(String columnHeaderName, InputStream stream,
            CloseableResourcePool closeableResourcePool) {
        try {
            List<String> columnFields = new ArrayList<>();
            InputStreamReader reader = new InputStreamReader(
                    new BOMInputStream(stream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                            ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                    StandardCharsets.UTF_8);
            CSVFormat format = LECSVFormat.format;
            CSVParser parser = new CSVParser(reader, format);
            Iterator<CSVRecord> csvRecordIterator = parser.iterator();
            closeableResourcePool.addCloseable(parser);

            if (!csvRecordIterator.hasNext()) {
                throw new LedpException(LedpCode.LEDP_18110);
            }
            int i = 0;
            boolean oneColumnMalformedCSV = false;
            while (i < MAX_NUM_ROWS && csvRecordIterator.hasNext()) {
                try {
                    String columnField = csvRecordIterator.next().get(columnHeaderName);
                    if (columnField != null && !columnField.isEmpty()) {
                        columnFields.add(columnField);
                    }
                    i++;
                } catch (IllegalArgumentException exp) {
                    oneColumnMalformedCSV = true;
                    continue;
                }
            }
            if (oneColumnMalformedCSV) {
                log.warn(String.format(
                        "One row for column: %s in the csv caused a csv parsing error, this might be due to a malformed csv",
                        columnHeaderName));
            }

            return columnFields;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
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
                sb.append(String.format(
                        "In file %s, cannot have columns %s as CSV headers because they correspond to the same information (%s)\n",
                        fileDisplayName, StringUtils.join(entry.getValue()), entry.getKey()));
            }
            throw new LedpException(LedpCode.LEDP_18107, new String[] { sb.toString() });
        }
    }

    public static void checkForEmptyHeaders(String fileDisplayName, Set<String> headerFields) {
        for (final String field : headerFields) {
            if (StringUtils.isEmpty(field)) {
                throw new LedpException(LedpCode.LEDP_18096, new String[] { fileDisplayName });
            }
        }
    }

    /**
     * Check if any CSV header name is longer than {@link ValidateFileHeaderUtils#MAX_HEADER_LENGTH}
     * @param headerFields set of csv header names to be checked
     * @throws LedpException with code {@link LedpCode#LEDP_18188} if any of the headers too long
     */
    public static void checkForLongHeaders(Set<String> headerFields) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (String field : headerFields) {
            if (StringUtils.length(field) > MAX_HEADER_LENGTH) {
                map.put(field, StringUtils.length(field));
            }
        }
        if (MapUtils.isNotEmpty(map)) {
            StringBuilder sb = new StringBuilder();
            map.entrySet().forEach(
                    entry -> sb.append(String.format("\nfield: %s, length: %s", entry.getKey(), entry.getValue())));
            throw new LedpException(LedpCode.LEDP_18188,
                    new String[] { String.valueOf(MAX_HEADER_LENGTH), sb.toString() });
        }
    }

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
            throw new LedpException(LedpCode.LEDP_18120);
        }
    }

    public static void checkForHeaderNum(Set<String> headerFields, int limit) {
        if (headerFields.size() > limit) {
            throw new LedpException(LedpCode.LEDP_18226, new String[] {String.valueOf(limit), String.valueOf(headerFields.size())});
        }

    }

    public static String convertFieldNameToAvroFriendlyFormat(String fieldName) {
        int length = fieldName.length();
        if (length == 0) {
            throw new SchemaParseException("Empty name");
        }
        StringBuilder sb = new StringBuilder();
        char first = fieldName.charAt(0);
        if (!(Character.isLetter(first) || first == '_')) {
            sb.append(AVRO_FIELD_NAME_PREFIX);
        }
        return sb.append(fieldName).toString().replaceAll("[^A-Za-z0-9_]", "_");
    }

    public static void checkForReservedHeaders(String displayName, Set<String> headerFields,
            Collection<String> reservedWords, Collection<String> reservedBeginings) {
        List<String> overlappedWords = new ArrayList<>();
        for (String reservedWord : reservedWords) {
            if (headerFields.contains(reservedWord)) {
                overlappedWords.add(reservedWord);
            }
        }
        if (!overlappedWords.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_18122, new String[] { overlappedWords.toString(), displayName });
        }
        for (String reservedBegining : reservedBeginings) {
            for (String header : headerFields) {
                if (Pattern.matches(reservedBegining.toLowerCase() + "\\d*", header.toLowerCase())) {
                    throw new LedpException(LedpCode.LEDP_18183, new String[] { header });
                }
            }
        }

    }

    public static void checkForCSVInjectionInFileNameAndHeaders(String fileDisplayName, Set<String> headers) {
        if (CSV_INJECTION_CHARACHTERS.indexOf(fileDisplayName.charAt(0)) != -1) {
            throw new LedpException(LedpCode.LEDP_18208);
        }
        for (String header : headers) {
            if (StringUtils.isNotBlank(header) && CSV_INJECTION_CHARACHTERS.indexOf(header.charAt(0)) != -1) {
                throw new LedpException(LedpCode.LEDP_18208);
            }
        }
    }
}
