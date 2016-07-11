package com.latticeengines.db.exposed.schemagen.postprocess;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLServerPostProcessor extends PostProcessor {

    public SQLServerPostProcessor() {
        registerInnerProcessor(new CreateIndexForForeignKey());
    }

    private static class CreateIndexForForeignKey implements LineProcessor {

        private static final Pattern REGEX = Pattern
                .compile("alter table (\\S+) add constraint (\\S+) foreign key \\((\\S+)\\) references (\\S+)");

        @Override
        public List<String> processLine(String line) {
            Matcher matcher = REGEX.matcher(line);
            if (matcher.find()) {
                MatchResult result = matcher.toMatchResult();
                String table = result.group(1);
                String constraint = result.group(2);
                String columns = result.group(3);

                String indexCreate = String.format("create nonclustered index %s on %s (%s)", //
                        constraint, table, columns);
                return Arrays.asList(line, indexCreate);
            }
            return Collections.singletonList(line);
        }
    }
}
