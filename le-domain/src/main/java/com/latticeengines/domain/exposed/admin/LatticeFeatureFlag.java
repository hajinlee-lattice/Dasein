package com.latticeengines.domain.exposed.admin;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;

public enum LatticeFeatureFlag {

    DANTE("Dante", "Dante"), //
    QUOTA("Quota", "Quota"), //
    TARGET_MARKET("TargetMarket", "Target Market"), //
    USE_EAI_VALIDATE_CREDENTIAL("ValidateCredsUsingEai", "Use Eai to valiate source credentials"), //
    ENABLE_POC_TRANSFORM("EnablePocTransform", "enable POC in data transform");

    private String name;
    private String documentation;
    private static Set<String> names;

    LatticeFeatureFlag(String name, String documentation) {
        this.name = name;
        this.documentation = documentation;
    }

    public String getName() {
        return this.name;
    }

    public String getDocumentation() {
        return this.documentation;
    }

    static {
        names = new HashSet<String>();
        for (PlsFeatureFlag flag : PlsFeatureFlag.values()) {
            names.add(flag.getName());
        }
    }

}
