package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import cascading.tuple.coerce.Coercions;
import cascading.tuple.coerce.Coercions.Coerce;

@Component
public class BomboraFirehoseFieldMapping implements CsvToAvroFieldMapping, Serializable {
    private static final long serialVersionUID = -4870380826193940885L;
	private List<FieldTuple> fieldTuples;
    private Map<String, String> csvToAvroFieldMap;
    private Map<String, String> avroToCsvFieldMap;
    private Map<String, Coerce<?>> csvFieldToTypeMap;

    public BomboraFirehoseFieldMapping() {
        initTuples();
        initMaps();
    }

    private List<FieldTuple> initTuples() {
        fieldTuples = new ArrayList<>();
        fieldTuples.add(new FieldTuple("Hashed Email / ID (Hex)", "HashedEmailIDHex", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Hashed Email / ID (Base64)", "HashedEmailIDBase64", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Domain", "Domain", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Interaction Type", "InteractionType", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 1", "Topic1", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 1 Score", "Topic1Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 2", "Topic2", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 2 Score", "Topic2Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 3", "Topic3", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 3 Score", "Topic3Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 4", "Topic4", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 4 Score", "Topic4Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 5", "Topic5", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 5 Score", "Topic5Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 6", "Topic6", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 6 Score", "Topic6Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 7", "Topic7", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 7 Score", "Topic7Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 8", "Topic8", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 8 Score", "Topic8Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 9", "Topic9", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 9 Score", "Topic9Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Topic 10", "Topic10", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Topic 10 Score", "Topic10Score", Coercions.DOUBLE_OBJECT));
        fieldTuples.add(new FieldTuple("Universal Date/Time", "UniversalDateTime", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Country", "Country", Coercions.STRING));
        fieldTuples.add(new FieldTuple("State/Region", "StateRegion", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Postal Code", "PostalCode", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Localized Date/Time", "LocalizedDateTime", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Custom ID", "CustomID", Coercions.STRING));
        fieldTuples.add(new FieldTuple("Source ID", "SourceID", Coercions.STRING));
        return null;
    }

    private void initMaps() {
        csvToAvroFieldMap = new HashMap<String, String>();
        avroToCsvFieldMap = new HashMap<String, String>();
        csvFieldToTypeMap = new HashMap<String, Coerce<?>>();

        populateMaps();
    }

    private void populateMaps() {
        for (FieldTuple tuple : fieldTuples) {
            csvFieldToTypeMap.put(tuple.getCsvFieldName(), tuple.getFieldType());
            csvToAvroFieldMap.put(tuple.getCsvFieldName(), tuple.getAvroFieldName());
            avroToCsvFieldMap.put(tuple.getAvroFieldName(), tuple.getCsvFieldName());
        }
    }

    @Override
    public String getAvroFieldName(String csvFieldName) {
        return csvToAvroFieldMap.get(csvFieldName);
    }

    @Override
    public String getCsvFieldName(String avroFieldName) {
        return avroToCsvFieldMap.get(avroFieldName);
    }

    @Override
    public Coerce<?> getFieldType(String csvFieldName) {
        return csvFieldToTypeMap.get(csvFieldName);
    }

    class FieldTuple implements Serializable {
        private static final long serialVersionUID = 8195760208322606390L;
		private String csvFieldName;
        private String avroFieldName;
        private Coerce<?> fieldType;

        public FieldTuple(String csvFieldName, String avroFieldName, Coerce<?> fieldType) {
            this.csvFieldName = csvFieldName;
            this.avroFieldName = avroFieldName;
            this.fieldType = fieldType;
        }

        public String getCsvFieldName() {
            return csvFieldName;
        }

        public String getAvroFieldName() {
            return avroFieldName;
        }

        public Coerce<?> getFieldType() {
            return fieldType;
        }
    }

}
