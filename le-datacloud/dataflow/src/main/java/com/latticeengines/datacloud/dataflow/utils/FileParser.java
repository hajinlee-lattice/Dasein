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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class FileParser {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(FileParser.class);

    private static final String[] METRO_CODES_HEADER = { "﻿New Metro Name", "Country" };

    @SuppressWarnings("resource")
    public static Map<String, List<NameLocation>> parseBomboraMetroCodes() {
        Map<String, List<NameLocation>> locationMap = new HashMap<>();
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("etl/AllUniqueMetroCodes.csv");
        if (is == null) {
            throw new RuntimeException("Cannot find resource etl/AllUniqueMetroCodes.csv");
        }
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(METRO_CODES_HEADER).withRecordSeparator("\n");
        try {
            CSVParser csvFileParser = new CSVParser(new InputStreamReader(is), csvFileFormat);
            List<CSVRecord> csvRecords = csvFileParser.getRecords();
            for (int i = 1; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);
                String metroArea = record.get(METRO_CODES_HEADER[0]);
                String country = record.get(METRO_CODES_HEADER[1]);
                metroArea = standardizeBomboraMetroArea(metroArea);
                if (metroArea != null) {
                    List<NameLocation> locations = extractLocFromBomboraMetro(metroArea, country);
                    locationMap.put(metroArea, locations);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse AllUniqueMetroCodes.csv", e);
        }
        return locationMap;
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
}
