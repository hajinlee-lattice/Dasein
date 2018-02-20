package com.latticeengines.db.exposed.schemagen.postprocess;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySQLPostProcessor extends PostProcessor {

    public MySQLPostProcessor() {
        registerInnerProcessor(new FixJsonColumnDefinition());
    }

    private static class FixJsonColumnDefinition implements PostProcessor.LineProcessor {

        private static final Pattern REGEX = Pattern.compile(".*`JSON`.*");

        @Override
        public List<String> processLine(String line) {
            Matcher matcher = REGEX.matcher(line);
            if (matcher.find()) {
                String newLine = line.replace("`JSON`", "JSON");
                return Collections.singletonList(newLine);
            }
            return Collections.singletonList(line);
        }
    }

}
