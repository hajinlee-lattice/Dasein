package com.latticeengines.domain.exposed.datacloud.transformation.config.dunsredirect;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class DunsRedirectBookConfig extends TransformerConfig {

    public static final String DUNS = "Duns";
    public static final String TARGET_DUNS = "TargetDuns";
    public static final String KEY_PARTITION = "KeyPartition";
    public static final String BOOK_SOURCE = "BookSource";

    @NotNull
    @NotEmptyString
    @JsonProperty("BookSource")
    private String bookSource;

    public String getBookSource() {
        return bookSource;
    }

    public void setBookSource(String bookSource) {
        this.bookSource = bookSource;
    }

}
