package com.latticeengines.camille.exposed.util;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Document;

public class DocumentUtils {
    public static <T> Document toDocument(T object) {
        return new Document(JsonUtils.serialize(object));
    }
    
    public static <T> T toObject(Document document, Class<T> clazz) {
        return JsonUtils.deserialize(document.getData(), clazz);
    }
}
