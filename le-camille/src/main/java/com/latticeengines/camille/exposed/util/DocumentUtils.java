package com.latticeengines.camille.exposed.util;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.VersionedDocument;

public final class DocumentUtils {

    protected DocumentUtils() {
        throw new UnsupportedOperationException();
    }
    public static <T> Document toRawDocument(T typesafe) {
        if (typesafe instanceof VersionedDocument) {
            VersionedDocument casted = (VersionedDocument) typesafe;
            return new Document(JsonUtils.serialize(typesafe), casted.getDocumentVersion());
        } else {
            return new Document(JsonUtils.serialize(typesafe));
        }
    }

    public static <T> T toTypesafeDocument(Document raw, Class<T> clazz) {
        if (raw == null) {
            return null;
        } else if (VersionedDocument.class.isAssignableFrom(clazz)) {
            T toReturn = JsonUtils.deserialize(raw.getData(), clazz);
            VersionedDocument casted = (VersionedDocument) toReturn;
            casted.setDocumentVersion(raw.getVersion());
            return toReturn;
        } else {
            return JsonUtils.deserialize(raw.getData(), clazz);
        }
    }
}
