package com.latticeengines.query.template;

import static com.google.common.collect.ImmutableSet.copyOf;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;

public class TemplateKeywords {

        private TemplateKeywords() { }

        private static Set<String> readLines(String path) {
            try {
                return copyOf(Resources.readLines(
                        TemplateKeywords.class.getResource("/template-keywords/" + path), Charsets.UTF_8,
                        new CommentDiscardingLineProcessor()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public static final Set<String> SPARKSQL = readLines("spark-sql");

        private static class CommentDiscardingLineProcessor implements LineProcessor<Collection<String>> {

            private final Collection<String> result = Sets.newHashSet();

            @Override
            public boolean processLine(String line) throws IOException {
                if (!line.isEmpty() && !line.startsWith("#")) {
                    result.add(line);
                }
                return true;
            }

            @Override
            public Collection<String> getResult() {
                return result;
            }

        }
    }
