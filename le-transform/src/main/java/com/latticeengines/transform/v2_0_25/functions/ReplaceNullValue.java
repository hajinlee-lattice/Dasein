package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class ReplaceNullValue implements RealTimeTransform {

    private static final long serialVersionUID = -7933032058139360251L;

    private static final Pattern patternDictionary = Pattern.compile("^\\{(.*)\\}$");
    private static final Pattern patternKeyAndValue = Pattern
            .compile("^u?(\'(.*?)\'|\"(.*?)\"): ([-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?)(, u?(\'|\").*|$)");

    protected Map<String, Object> lookupMap = new HashMap<>();

    public ReplaceNullValue() {
    }

    public ReplaceNullValue(String modelPath) {
        importLoookupMapFromPythonString(modelPath + "/imputations.txt");
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);
        if (o == null) {
            return lookupMap.get(column);
        }
        return o;
    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
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
                        String.format("Text file with imputation values has no dictionary: %s; text string is\n%s",
                                filename, pythonString));
            }

            while (true) {
                Matcher matcherKeyAndValue = patternKeyAndValue.matcher(dictionaryString);
                if (matcherKeyAndValue.matches()) {
                    String key = matcherKeyAndValue.group(2);
                    if (key == null) {
                        key = matcherKeyAndValue.group(3);
                    } else {
                        key = key.replace("\\\'", "\'");
                    }

                    Double value = Double.parseDouble(matcherKeyAndValue.group(4));
                    lookupMap.put(key, value);
                    dictionaryString = matcherKeyAndValue.group(6);
                    if (!dictionaryString.equals("")) {
                        dictionaryString = dictionaryString.substring(2);
                    } else {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot open text file with imputation values: %s", filename), e);
        }
    }

}
