package com.latticeengines.camille.exposed.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

final class LifecycleUtils {

    protected LifecycleUtils() {
        throw new UnsupportedOperationException();
    }
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    static void validateIds(String... ids) {
        for (String id : ids)
            validateId(id);
    }

    private static void validateId(String id) {
        if (id == null) {
            IllegalArgumentException e = new IllegalArgumentException("Id cannot be null");
            log.error(e.getMessage(), e);
            throw e;
        }

        if (id.contains(".")) {
            IllegalArgumentException e = new IllegalArgumentException(MessageFormatter.format(
                    "Id={}.  Ids cannot contain periods", id).getMessage());
            log.error(e.getMessage(), e);
            throw e;
        }
    }
}
