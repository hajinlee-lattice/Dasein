package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.scheme.util.DelimitedParser;
import cascading.scheme.util.FieldTypeResolver;

public class CustomDelimitedParserSpecialEqualQuote extends DelimitedParser {
    private static final Logger log = LoggerFactory.getLogger(CustomDelimitedParserSpecialEqualQuote.class);

    private static final long serialVersionUID = -1832800305799711109L;
    private static final String COMMA = ",";
    private static final String QUOTE = "\"";
    private static final String EQUAL_QUOTE = "=" + QUOTE;

    private CsvToAvroFieldMapping fieldMap;

    public CustomDelimitedParserSpecialEqualQuote(CsvToAvroFieldMapping fieldMap, String delimiter, String quote, boolean strict,
            boolean safe, FieldTypeResolver fieldTypeResolver) {
        super(delimiter, quote, null, strict, safe, fieldTypeResolver);
        this.fieldMap = fieldMap;
    }

    @Override
    protected Type[] inferTypes(Object[] result) {
        if (result == null || result.length == 0) {
            return null;
        }
        Type[] types = new Type[result.length];
        for (int i = 0; i < result.length; i++) {
            types[i] = fieldMap.getFieldType((String) result[i]);

        }
        return types;
    }

    @Override
    public String[] createSplit(String value, Pattern splitPattern, int numValues) {
        value = preProcessValue(value);
        String[] split = splitPattern.split(value, numValues);
        return split;
    }

    // if needed, fix input string to let standard regex parser to
    // work fine and not hang
    // example 1: abc="fff"|def="ddd",pqr => abc="fff",pqr
    // example 1: abc="ff,f"|def="ddd",pqr => abc="ff,f",pqr
    // example 1: abc="fff"|def="ddd" => abc="fff"
    static String preProcessValue(String value) {
        String originalValue = value;
        try {
            if (value.contains(EQUAL_QUOTE) && (value.indexOf(EQUAL_QUOTE) != value.lastIndexOf(EQUAL_QUOTE))) {
                log.debug(
                        "We need to preprocess this line as it may potentially lead the regex parser to hang for hours: "
                                + value);

                List<String> basicSplits = new ArrayList<>();
                int quoteCount = 0;
                String runningSplit = "";
                for (int i = 0; i < value.length(); i++) {
                    char c = value.charAt(i);
                    runningSplit += c;
                    if (c == QUOTE.charAt(0)) {
                        quoteCount++;
                    } else if (c == COMMA.charAt(0)) {
                        if (quoteCount % 2 == 0) {
                            basicSplits.add(runningSplit.substring(0, runningSplit.length() - COMMA.length()));
                            runningSplit = "";
                        }
                    }
                }

                basicSplits.add(runningSplit);
                value = "";

                for (int i = 0; i < basicSplits.size(); i++) {
                    String basicSplit = basicSplits.get(i);
                    basicSplit = inspectFieldValue(basicSplit);
                    value += (i == 0 ? "" : COMMA) + basicSplit;
                }

                log.debug("Modified input line to make it compiant with regex parser: " + value);
            }

            return value;
        } catch (Exception ex) {
            log.error("Could not preprocess line, reverting to original line: " + ex.getMessage(), ex);
            return originalValue;
        }
    }

    private static String inspectFieldValue(String basicSplit) {
        if (basicSplit.contains(EQUAL_QUOTE)
                && (basicSplit.indexOf(EQUAL_QUOTE) != basicSplit.lastIndexOf(EQUAL_QUOTE))) {
            String originalSplit = basicSplit;
            int index1 = basicSplit.indexOf(EQUAL_QUOTE);
            int secondQuoteIdx = basicSplit.indexOf(QUOTE, index1 + EQUAL_QUOTE.length());
            basicSplit = basicSplit.substring(0, secondQuoteIdx + QUOTE.length());
            log.debug("Replace input field: {" + originalSplit + "} with {" + basicSplit + "}");
        }
        return basicSplit;
    }
}
