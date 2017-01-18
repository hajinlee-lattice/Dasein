package com.latticeengines.domain.exposed.attribute;

public class EnrichmentAttributeFlags extends AttributeFlags {
    private boolean selected;

    public boolean isSelected() {
        return selected;
    }

    public void setSelected(boolean selected) {
        this.selected = selected;
    }
}
