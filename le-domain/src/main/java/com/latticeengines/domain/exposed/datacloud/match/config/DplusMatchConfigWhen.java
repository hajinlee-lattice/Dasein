package com.latticeengines.domain.exposed.datacloud.match.config;

import com.google.common.base.Preconditions;

public class DplusMatchConfigWhen {

    private final DplusMatchConfig config;
    private final DplusMatchConfig.SpeicalRule predicate;

    DplusMatchConfigWhen(DplusMatchConfig config, DplusMatchConfig.SpeicalRule predicate) {
        this.config = config;
        this.predicate = predicate;
    }

    public DplusMatchConfig apply(DplusMatchRule rule) {
        Preconditions.checkNotNull(predicate, "Not in a state for appending new special rule.");
        predicate.setSpecialRule(rule);
        config.addSpecialRule(predicate);
        return config;
    }

}
