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

import org.apache.avro.SchemaParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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

    public static final ImmutableList<String> CDL_RESERVED_FIELDS =
            ImmutableList.of("AdvertisingTechnologiesTopAttributes", "AnalyticsTechnologiesTopAttributes", "AudioVideoMediaTechnologiesTopAttributes", "CABLE_TELEX_NUMBER", "CHIEF_EXECUTIVE_OFFICER_NAME", "CHIEF_EXECUTIVE_OFFICER_TITLE", "CLUSTER_CRS", "CLUSTER_TPS", "COMPOSITE_RISK_SCORE", "ContentDeliveryNetworksTopAttributes", "ContentManagementSystemsTopAttributes", "COUNTRY_ACCESS_CODE", "CREDIT_CARD_RESPONSE_R", "CRMAlert", "CURRENCY_CODE", "DataFeedsTechnologiesTopAttributes", "DIAS_CODE", "DNSProvidersTopAttributes", "DomainParkingTopAttributes", "DOMESTIC_HQ_SALES_VOLUME", "EcommerceTechnologiesTopAttributes", "EmailProvidersTopAttributes", "EMPLOYEES_HERE", "EMPLOYEES_HERE_RELIABILITY_CODE", "EMPLOYEES_TOTAL", "EMPLOYEES_TOTAL_RELIABILITY_CODE", "Facebook_Likes", "Facebook_Url", "FACSIMILE_NUMBER", "FULL_REPORT_DATE", "GLOBAL_HQ_SALES_VOLUME", "GooglePlus_Url", "HEADQUARTER_PARENT_BUSINESS_NAME", "HEADQUARTER_PARENT_CITY_NAME", "HEADQUARTER_PARENT_COUNTRY_NAME", "HEADQUARTER_PARENT_DnB_CITY_CODE", "HEADQUARTER_PARENT_DnB_CONTINENT", "HEADQUARTER_PARENT_DnB_COUNTRY_CODE", "HEADQUARTER_PARENT_DnB_COUNTY_CODE", "HEADQUARTER_PARENT_DUNS_NUMBER", "HEADQUARTER_PARENT_POSTAL_CODE", "HEADQUARTER_PARENT_STATE_PROVINCE", "HEADQUARTER_PARENT_STATE_PROV_ABR", "HEADQUARTER_PARENT_STREET_ADDRESS", "HIERARCHY_CODE", "HostingProvidersTopAttributes", "HOTLIST_ADDRESS_CHAN", "HOTLIST_CEO_CHANGE", "HOTLIST_COMPANY_NAME_CHG", "HOTLIST_NEW", "HOTLIST_OWNERSHIP_CH", "HOTLIST_TELEPHONE_NUMBER_CH", "HPAEmailSuffix", "HPANumPages", "IMPORT_EXPORT_AGENT_CODE", "IsMatched", "IsPublicDomain", "JavascriptLibrariesTopAttributes", "Last_Funding_Round_Amount", "Last_Funding_Round_Year", "LAST_UPDATE_DATE", "LatticeAccountId", "LINE_OF_BUSINESS", "LinkedIn_Url", "LOCAL_ACTIVITY_TYPE_CODE", "MAILING_ADDRESS", "MAILING_CITY_NAME", "MAILING_COUNTRY_NAME", "MAILING_COUNTY_NAME", "MAILING_POSTAL_CODE", "MAILING_STATE_PROVINCE_ABBR", "MAILING_STATE_PROVINCE_NAME", "MapsPluginsTopAttributes", "MarketingAutomationAlert", "MarketingAutomationTopAttributes", "NATIONAL_IDENTIFICATION_NUMBER", "NATIONAL_IDENTIFICATION_TYPE_CODE", "NUMBER_OF_FAMILY_MEMBERS", "OperatingSystemsAndServersTopAttributes", "OUT_OF_BUSINESS_INDICATOR", "PARENTS_SALES_VOLUME", "PaymentProvidersTopAttributes", "PREMIUM_MARKETING_PRESCREEN", "PREVIOUS_DUNS_NUMBER", "PRIMARY_LOCAL_ACTIVITY_CODE", "PRINCIPALS_INCLUDED_INDICATOR", "PROPENSITY_TO_HAVE_A_LEASE_ACCOUNT", "PROPENSITY_TO_HAVE_A_LINE_OF_CRE", "RECORD_SOURCE_CODE", "REGISTERED_ADDRESS_INDICATOR", "SALES_VOLUME_LOCAL_CURRENCY", "SALES_VOLUME_RELIABILITY_CODE", "SALES_VOLUME_US_DOLLARS", "SSLCertificatesTopAttributes", "STATE_PROVINCE_ABBR", "STATUS_CODE", "STREET_ADDRESS_2", "SUBSIDIARY_INDICATOR", "SystemAlert", "TechnologiesMonthlySpend", "TELEPHONE_NUMBER", "Total_Amount_Raised", "TOTAL_CREDIT_BALANCE_RANKING_CR", "TRADESTYLE_NAME", "TRIPLE_PLAY_SEGMENT", "Twitter_Followers", "US_1987_SIC_1", "US_1987_SIC_2", "US_1987_SIC_3", "US_1987_SIC_4", "US_1987_SIC_5", "US_1987_SIC_6", "WebMasterRegistrationsTopAttributes", "WebServersTopAttributes", "WebsiteDocumentStandardsTopAttributes", "WebsiteEncodingStandardsTopAttributes", "WebsiteFrameworksTopAttributes", "WidgetsTopAttributes", "YEAR_STARTED");
    public static final ImmutableList<String> CDL_RESERVED_PREFIX =
            ImmutableList.of("LDC_", "BmbrSurge_", "Alexa", "Bmbr30_", "TechIndicator_", "CloudTechnologies_", "FeatureTerm", "dnb_", "BusinessTechnologies", "DOMESTIC_ULTIMATE_", "LE_", "Semrush", "GLOBAL_ULTIMATE_", "Feature");

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
        for (String field : headerFields) {
            if (StringUtils.length(field) > MAX_HEADER_LENGTH) {
                throw new LedpException(LedpCode.LEDP_18188, new String[] { String.valueOf(MAX_HEADER_LENGTH), field });
            }
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
}
