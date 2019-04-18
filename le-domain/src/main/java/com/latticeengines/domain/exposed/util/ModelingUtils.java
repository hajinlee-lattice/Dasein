package com.latticeengines.domain.exposed.util;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.KV;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;

public class ModelingUtils {

    public static final PivotValuesLookup getPivotValues(Configuration yarnConfiguration,
            String pivotArtifactPath) throws Exception {
        Map<String, AbstractMap.Entry<String, List<String>>> pivotValuesByTargetColumn = new HashMap<>();
        Map<String, List<AbstractMap.Entry<String, String>>> pivotValuesBySourceColumn = new HashMap<>();
        Map<String, UserDefinedType> sourceColumnTypes = new HashMap<>();

        if (StringUtils.isNotEmpty(pivotArtifactPath)) {
            try (Reader reader = new InputStreamReader(
                    new BOMInputStream(HdfsUtils.getInputStream(yarnConfiguration, //
                            pivotArtifactPath)),
                    "UTF-8")) {
                try (CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader())) {
                    for (CSVRecord record : parser) {
                        String targetColumn = record.get("TargetColumn");
                        String sourceColumn = record.get("SourceColumn");
                        String value = record.get("Value");
                        String sourceColumnType = record.get("SourceColumnType");

                        UserDefinedType userType = UserDefinedType.valueOf(sourceColumnType);

                        if (userType == null) {
                            throw new LedpException(LedpCode.LEDP_10010,
                                    new String[] { sourceColumnType });
                        }
                        sourceColumnTypes.put(sourceColumn, userType);

                        Map.Entry<String, List<String>> p = pivotValuesByTargetColumn
                                .get(targetColumn);
                        List<AbstractMap.Entry<String, String>> s = pivotValuesBySourceColumn
                                .get(sourceColumn);
                        if (p == null) {
                            List<String> l = new ArrayList<>();
                            l.add(value);
                            p = new AbstractMap.SimpleEntry<String, List<String>>(sourceColumn, l);
                            pivotValuesByTargetColumn.put(targetColumn, p);
                        } else {
                            p.getValue().add(value);
                        }

                        if (s == null) {
                            s = new ArrayList<>();
                            pivotValuesBySourceColumn.put(sourceColumn, s);
                        }
                        s.add(new AbstractMap.SimpleEntry<String, String>(targetColumn, value));
                    }
                }

            }
        }
        return new PivotValuesLookup(pivotValuesByTargetColumn, pivotValuesBySourceColumn,
                sourceColumnTypes);
    }

    public static String addPivotValuesToMetadataContent(ModelingMetadata metadata,
            PivotValuesLookup pivotValues) {
        Map<String, List<AbstractMap.Entry<String, String>>> pivotValuesBySourceColumn = pivotValues.pivotValuesBySourceColumn;
        Map<String, UserDefinedType> sourceColumnTypes = pivotValues.sourceColumnToUserType;

        for (Map.Entry<String, List<AbstractMap.Entry<String, String>>> entry : pivotValuesBySourceColumn
                .entrySet()) {
            final String name = entry.getKey();
            UserDefinedType userType = sourceColumnTypes.get(name);

            AttributeMetadata attrMetadatum = Iterables.find(metadata.getAttributeMetadata(),
                    new Predicate<AttributeMetadata>() {
                        @Override
                        public boolean apply(AttributeMetadata attr) {
                            return attr.getColumnName().equals(name);
                        }
                    }, null);

            if (attrMetadatum == null) {
                continue;
            }
            // build extension for pivot values
            List<Map<String, ?>> pValues = new ArrayList<>();

            for (Map.Entry<String, String> e : entry.getValue()) {
                Map<String, Object> values = new HashMap<>();
                values.put("PivotColumn", e.getKey());
                values.put("PivotValue", userType.cast(e.getValue()));
                pValues.add(values);
            }

            List<KV> extensions = new ArrayList<>();
            extensions.addAll(attrMetadatum.getExtensions());
            extensions.add(new KV("PivotValues", pValues));
            attrMetadatum.setExtensions(extensions);
        }
        return JsonUtils.serialize(metadata);
    }
}
