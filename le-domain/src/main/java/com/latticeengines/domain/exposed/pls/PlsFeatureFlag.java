package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonValue;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;

public enum PlsFeatureFlag {
    ACTIVATE_MODEL_PAGE("ActivateModelPage", "the page to manage model activities through segments."), SYSTEM_SETUP_PAGE(
            "SystemSetupPage", "System Setup page."), ADMIN_ALERTS_TAB("AdminAlertsTab",
            "Alerts tab in the admin page."), SETUP_PAGE("SetupPage", "Root flag for the whole Setup page."),
            DEPLOYMENT_WIZARD_PAGE("DeploymentWizardPage", "Root flag for the deployment wizard page.");

    private String name;
    private FeatureFlagDefinition definition;
    private static List<String> names;

    static {
        names = new ArrayList<>();
        for (PlsFeatureFlag flag : PlsFeatureFlag.values()) {
            names.add(flag.getName());
        }
    }

    PlsFeatureFlag(String name, String documentation) {
        this.name = name;
        this.definition = new FeatureFlagDefinition();
        definition.setDisplayName(name);
        definition.setDocumentation(documentation);
    }

    public static PlsFeatureFlag fromName(String name) {
        return PlsFeatureFlag.valueOf(name.toUpperCase());
    }

    public static List<String> getNames() {
        return names;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    @JsonValue
    public FeatureFlagDefinition getDefinition() {
        return definition;
    }

    @Override
    public String toString() {
        return name;
    }
}
