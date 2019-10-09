package com.latticeengines.db.exposed.schemagen.postprocess;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySQLPostProcessor extends PostProcessor {

    public MySQLPostProcessor() {
        registerInnerProcessor(new FixCustomColumnDefinition());
    }

    static class FixCustomColumnDefinition implements PostProcessor.LineProcessor {

        private static final Pattern REGEX = Pattern.compile("`'.*?(GENERATED|JSON|DEFAULT).*?'`");

        @Override
        public List<String> processLine(String line) {
            Matcher matcher;
            do {
                matcher = REGEX.matcher(line);
                if (matcher.find()) {
                    String matched = matcher.group(0);
                    // remove `' and '`
                    String formatted = matched.substring(matched.indexOf("`'") + 2);
                    formatted = formatted.substring(0, formatted.lastIndexOf("'`"));
                    line = line.replace(matched, formatted);
                }
            } while (matcher.find());
            return Collections.singletonList(line);
        }
    }

}
