package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class Pivot implements RealTimeTransform {

    private static final long serialVersionUID = -3706913709622887686L;

    private static final Pattern patternDictionary = Pattern.compile("^\\{(.*)\\}$");
    private static final Pattern patternKeyAndValue = Pattern
            .compile("^u?(\'(.*?)\'|\"(.*?)\"): \\(u?(\'(.*?)\'|\"(.*?)\"), \\[(.*?)\\]\\)(, u?(\'|\").*|$)");
    private static final Pattern patternValues = Pattern.compile("^u?(\'(.*?)\'|\"(.*?)\")(, u?(\'|\").*|$)");

    protected Map<String, Object> lookupMap = new HashMap<>();

    public Pivot() {
    }

    public Pivot(String modelPath) {
        importLoookupMapFromPythonString(modelPath + "/pivotvalues.txt");
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column1");
        String targetColumn = (String) arguments.get("column2");
        List<?> values = (List) lookupMap.get(targetColumn);

        Object recordValue = record.get(column);
        if (targetColumn.endsWith("___ISNULL__")) {

            if (recordValue == null) {
                return 1.0;
            }
            return 0.0;
        }

        if (values != null && values.size() > 0) {
            List<?> pivotValues = (List) values.get(1);
            for (Object value : pivotValues) {
                if (value == null) {
                    continue;
                }
                if (value.equals(recordValue)) {
                    return 1.0;
                }
            }
        }
        return 0.0;

    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
    }

    public Map<String, Object> getLookupMap() {
        return lookupMap;
    }

    private void importLoookupMapFromPythonString(String filename) {
        try {
            @SuppressWarnings("deprecation")
            String pythonString = FileUtils.readFileToString(new File(filename));
            String dictionaryString = "";
            Matcher matcherDictionary = patternDictionary.matcher(pythonString);
            if (matcherDictionary.matches()) {
                dictionaryString = matcherDictionary.group(1);
            } else {
                throw new RuntimeException(
                        String.format("Text file with pivot values has no dictionary: %s; text string is\n%s", filename,
                                pythonString));
            }

            while (true) {
                Matcher matcherKeyAndValue = patternKeyAndValue.matcher(dictionaryString);
                if (matcherKeyAndValue.matches()) {
                    String targetColKey = matcherKeyAndValue.group(2);
                    if (targetColKey == null) {
                        targetColKey = matcherKeyAndValue.group(3);
                    } else {
                        targetColKey = targetColKey.replace("\\\'", "\'");
                    }

                    String sourceColKey = matcherKeyAndValue.group(5);
                    if (sourceColKey == null) {
                        sourceColKey = matcherKeyAndValue.group(6);
                    } else {
                        sourceColKey = sourceColKey.replace("\\\'", "\'");
                    }

                    String sourceColValues = matcherKeyAndValue.group(7).replace("\\\'", "\'");

                    // Assume the values are of type String. A better
                    // implementation using json will
                    // replace this version.
                    List<String> values = new ArrayList<>();
                    while (Boolean.TRUE) {
                        Matcher matcherValues = patternValues.matcher(sourceColValues);
                        if (matcherValues.matches()) {
                            String value = matcherValues.group(2);
                            if (value == null) {
                                value = matcherValues.group(3);
                            }
                            values.add(value);
                            sourceColValues = matcherValues.group(4);
                            if (!sourceColValues.equals("")) {
                                sourceColValues = sourceColValues.substring(2);
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    List<Object> valueTuple = new ArrayList<>();
                    valueTuple.add(sourceColKey);
                    valueTuple.add(values);
                    lookupMap.put(targetColKey, valueTuple);
                    dictionaryString = matcherKeyAndValue.group(8);
                    if (!dictionaryString.equals("")) {
                        dictionaryString = dictionaryString.substring(2);
                    } else {
                        break;
                    }
                } else {
                    throw new RuntimeException(String.format(
                            "Text file with pivot values has dictionary with unexpected key/value: %s; text string is\n%s",
                            filename, dictionaryString));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot open text file with pivot values: %s", filename), e);
        }
    }

}
