package com.latticeengines.query.template;

import com.querydsl.core.types.Ops;
import com.querydsl.sql.SQLTemplates;

public class PrestoTemplates extends SQLTemplates {

    public static final PrestoTemplates DEFAULT = new PrestoTemplates();

    public static Builder builder() {
        return new Builder() {
            protected SQLTemplates build(char escape, boolean quote) {
                return new PrestoTemplates(escape, quote);
            }
        };
    }

    public PrestoTemplates() {
        this('\\', false);
    }

    public PrestoTemplates(boolean quote) {
        this('\\', quote);
    }

    public PrestoTemplates(char escape, boolean quote) {
        super(TemplateKeywords.PRESTO, "\"", escape, quote, false);
        setDummyTable(null);
        setCountDistinctMultipleColumns(true);
        setCountViaAnalytics(true);
        this.setDefaultValues("\ndefault values");
        setSupportsUnquotedReservedWordsAsIdentifier(true);

        //REM-NA add(Ops.LIKE_ESCAPE_IC, "{0} ilike {1} escape '{2s}'");
        // like without escape
        if (escape == '\\') {
            add(Ops.LIKE, "{0} like {1}");
            add(Ops.LIKE_IC, "lower({0}) like lower({1})");
            add(Ops.ENDS_WITH, "{0} like {%1}");
            add(Ops.ENDS_WITH_IC, "lower({0}) like '%' || lower({1})");
            add(Ops.STARTS_WITH, "{0} like {1%}");
            add(Ops.STARTS_WITH_IC, "lower({0}) like lower({1}) || '%'");
            add(Ops.STRING_CONTAINS, "{0} like {%1%}");
            add(Ops.STRING_CONTAINS_IC, "lower({0}) like '%' ||  lower({1}) || '%'");
        } else {
            throw new UnsupportedOperationException();
        }

    }

}
