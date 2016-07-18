package com.latticeengines.pls.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;

public class PivotMappingFileUtils {

    public static List<Attribute> createAttrsFromPivotSourceColumns(InputStream stream, List<Attribute> attrs) {
        List<Attribute> newAttrs = new ArrayList<>();
        newAttrs.addAll(attrs);
        try (InputStreamReader reader = new InputStreamReader(stream)) {
            CSVFormat format = LECSVFormat.format;
            try (CSVParser parser = new CSVParser(reader, format)) {
                String sourceColumnHeaderName = "SourceColumn";
                for (String key : parser.getHeaderMap().keySet()) {
                    if (key.equalsIgnoreCase("SourceColumn")) {
                        sourceColumnHeaderName = key;
                        break;
                    }
                }
                Iterator<CSVRecord> iterator = parser.iterator();
                while (iterator.hasNext()) {
                    final String sourceColumnName = iterator.next().get(sourceColumnHeaderName);
                    Iterables.find(attrs, new Predicate<Attribute>() {
                        @Override
                        public boolean apply(Attribute input) {
                            if (input.getDisplayName().equalsIgnoreCase(sourceColumnName)) {
                                input.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
                                return true;
                            }
                            return false;
                        }

                    }, null);
                }
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18115, new String[] { e.getMessage() });
        }
        return newAttrs;
    }
}
