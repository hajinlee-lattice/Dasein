package com.latticeengines.datacloud.dataflow.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class FileParser {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FileParser.class);

    private static final String[] BOMBORA_METRO_CODES_HEADER = { "﻿New Metro Name", "Country" };

    private static final String[] BOMBORA_INTENT_HEADER = { "CompositeScoreMin", "CompositeScoreMax", "Intent" };

    public static final String[] AM_PROFILE_CONFIG_HEADER = { "AMColumnID", "IsSegment", "DecodeStrategy",
            "EncodeBitUnit", "BucketAlgorithm" };

    @SuppressWarnings("resource")
    public static Map<String, List<NameLocation>> parseBomboraMetroCodes() {
        Map<String, List<NameLocation>> locationMap = new HashMap<>();
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("etl/BomboraUniqueMetroCodes.csv");
        if (is == null) {
            throw new RuntimeException("Cannot find resource etl/BomboraUniqueMetroCodes.csv");
        }
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(BOMBORA_METRO_CODES_HEADER).withRecordSeparator("\n");
        try {
            CSVParser csvFileParser = new CSVParser(new InputStreamReader(is), csvFileFormat);
            List<CSVRecord> csvRecords = csvFileParser.getRecords();
            for (int i = 1; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);
                String metroArea = record.get(BOMBORA_METRO_CODES_HEADER[0]);
                String country = record.get(BOMBORA_METRO_CODES_HEADER[1]);
                metroArea = standardizeBomboraMetroArea(metroArea);
                if (metroArea != null) {
                    List<NameLocation> locations = extractLocFromBomboraMetro(metroArea, country);
                    locationMap.put(metroArea, locations);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse BomboraUniqueMetroCodes.csv", e);
        }
        return locationMap;
    }

    @SuppressWarnings("resource")
    public static Map<Range<Integer>, String> parseBomboraIntent() {
        Map<Range<Integer>, String> intentMap = new HashMap<>();
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("etl/BomboraIntentCuration.csv");
        if (is == null) {
            throw new RuntimeException("Cannot find resource etl/BomboraIntentCuration.csv");
        }
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(BOMBORA_INTENT_HEADER).withRecordSeparator("\n");
        try {
            CSVParser csvFileParser = new CSVParser(new InputStreamReader(is), csvFileFormat);
            List<CSVRecord> csvRecords = csvFileParser.getRecords();
            for (int i = 1; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);
                Integer compoScoreMin = Integer.valueOf(record.get(BOMBORA_INTENT_HEADER[0]));
                Integer compoScoreMax = Integer.valueOf(record.get(BOMBORA_INTENT_HEADER[1]));
                String intent = record.get(BOMBORA_INTENT_HEADER[2]);
                intentMap.put(Range.between(compoScoreMin, compoScoreMax), intent);
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse AllUniqueMetroCodes.csv", e);
        }
        return intentMap;
    }

    private static String standardizeBomboraMetroArea(String metroArea) {
        if (StringUtils.isBlank(metroArea)) {
            return null;
        }
        metroArea = metroArea.replaceAll("\\(.*\\)", "");
        metroArea = metroArea.replace("\t", " ").replace("\n", "").replace("\r", "").trim();
        return metroArea;
    }

    /**
     * Sample metro codes:
     * Tampa / St.Pete / Sarasota , FL     --- 3 cities share same state
     * Tallahassee, FL / Thomasville, GA
     * Askim
     */
    private static List<NameLocation> extractLocFromBomboraMetro(String metroArea, String country) {
        List<NameLocation> locations = new ArrayList<>();
        String[] areaList = metroArea.split("/");
        String state = null;
        country = StringUtils.isNotBlank(country) ? country.trim() : null;
        for (int i = areaList.length - 1; i >= 0; i--) {
            String area = areaList[i];
            if (StringUtils.isBlank(area)) {
                continue;
            }
            String[] loc = area.split(",");
            if (loc.length == 0 || loc.length > 2) { // unrecognized format
                continue;
            }
            if (loc.length == 2) {
                state = loc[1];
                state = StringUtils.isNotBlank(state) ? state.trim() : null;
            }
            String city = loc[0];
            city = StringUtils.isNotBlank(city) ? city.trim() : null;
            NameLocation location = new NameLocation();
            location.setCountry(country);
            // Remove Chinese invalid province
            if (!(country != null && state != null && country.equalsIgnoreCase("China") && state.contains("CN-"))) {
                location.setState(state);
            }
            location.setCity(city);
            locations.add(location);
        }
        return locations;
    }

    /**
     * Temporary used before migrating am profiling job to table driven
     * transformer
     */
    public static Map<String, Map<String, Object>> parseAMProfileConfig() {
        Map<String, Map<String, Object>> configs = new HashMap<>();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("etl/AMProfileConfig.csv");
        if (is == null) {
            throw new RuntimeException("Cannot find resource etl/AMProfileConfig.csv");
        }
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(AM_PROFILE_CONFIG_HEADER).withRecordSeparator("\n")
                .withDelimiter('\t');
        try {
            CSVParser csvFileParser = new CSVParser(new InputStreamReader(is), csvFileFormat);
            List<CSVRecord> csvRecords = csvFileParser.getRecords();
            for (int i = 1; i < csvRecords.size(); i++) {
                Map<String, Object> config = new HashMap<>();
                CSVRecord record = csvRecords.get(i);
                String amId = record.get(AM_PROFILE_CONFIG_HEADER[0]);
                Boolean isSeg = Boolean.valueOf(record.get(AM_PROFILE_CONFIG_HEADER[1]));
                String decodeStrategy = StringUtils.isNotBlank(record.get(AM_PROFILE_CONFIG_HEADER[2]))
                        ? record.get(AM_PROFILE_CONFIG_HEADER[2]) : null;
                Integer encodeBitUnit = StringUtils.isBlank(record.get(AM_PROFILE_CONFIG_HEADER[3])) ? null
                        : Integer.valueOf(record.get(AM_PROFILE_CONFIG_HEADER[3]));
                String algo = StringUtils.isNotBlank(record.get(AM_PROFILE_CONFIG_HEADER[4]))
                        ? record.get(AM_PROFILE_CONFIG_HEADER[4]) : null;
                config.put(AM_PROFILE_CONFIG_HEADER[1], isSeg);
                config.put(AM_PROFILE_CONFIG_HEADER[2], decodeStrategy);
                config.put(AM_PROFILE_CONFIG_HEADER[3], encodeBitUnit);
                config.put(AM_PROFILE_CONFIG_HEADER[4], algo);
                configs.put(amId, config);
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse BomboraUniqueMetroCodes.csv", e);
        }
        return configs;
    }
}
