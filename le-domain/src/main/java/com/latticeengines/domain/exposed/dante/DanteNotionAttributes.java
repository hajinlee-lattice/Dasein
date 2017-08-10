package com.latticeengines.domain.exposed.dante;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DanteNotionAttributes {
    Map<String, List<DanteAttribute>> notionAttributes;

    List<String> invalidNotions;

    public Map<String, List<DanteAttribute>> getNotionAttributes() {
        return notionAttributes;
    }

    public void setNotionAttributes(Map<String, List<DanteAttribute>> notionAttributes) {
        this.notionAttributes = notionAttributes;
    }

    public List<String> getInvalidNotions() {
        return invalidNotions;
    }

    public void setInvalidNotions(List<String> invalidNotions) {
        this.invalidNotions = invalidNotions;
    }

    public void addNotion(String notion, List<DanteAttribute> attributes) {
        if (notionAttributes == null) {
            notionAttributes = new HashMap<>();
        }
        notionAttributes.put(notion, attributes);
    }

    public void addInvalidNotion(String notion) {
        if (invalidNotions == null) {
            invalidNotions = new ArrayList<>();
        }
        invalidNotions.add(notion);
    }
}
