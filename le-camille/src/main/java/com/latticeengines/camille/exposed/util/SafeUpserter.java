package com.latticeengines.camille.exposed.util;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class SafeUpserter {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public final long DEFAULT_RETRY_BACKOFF_MILLISECONDS = 50;
    public final int DEFAULT_NUM_RETRIES = 5;

    public SafeUpserter() {
        this.retryBackoffMilliseconds = DEFAULT_RETRY_BACKOFF_MILLISECONDS;
        this.numRetries = DEFAULT_NUM_RETRIES;
    }

    public SafeUpserter(long retryBackoffMilliseconds, int numRetries) {
        this.retryBackoffMilliseconds = retryBackoffMilliseconds;
        this.numRetries = numRetries;
    }

    public <S extends ConfigurationScope, T> void upsert(S scope, Path path, Function<T, T> upserter, Class<T> clazz) {
        ConfigurationController<S> controller = ConfigurationController.construct(scope);

        for (int remaining = numRetries; remaining > 0; remaining--) {
            try {
                if (!controller.exists(path)) {
                    createNewPathWithDoc(upserter, path, controller);
                } else {
                    Document existingRaw = controller.get(path);
                    if (StringUtils.isEmpty(existingRaw.getData())) {
                        controller.delete(path);
                        createNewPathWithDoc(upserter, path, controller);
                    } else {
                        T replacementTyped = upserter.apply(DocumentUtils.toTypesafeDocument(existingRaw, clazz));
                        if (replacementTyped == null) {
                            throw new NullPointerException("Upserter must not return null");
                        }
                        Document replacementRaw = DocumentUtils.toRawDocument(replacementTyped);
                        // Handle cases where T is not a VersionedDocument
                        replacementRaw.setVersion(existingRaw.getVersion());

                        controller.set(path, replacementRaw);
                    }
                }
                return;
            } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
                log.warn(String.format("Possibly temporary failure attempting to safely upsert to path %s", path), e);
                sleep((numRetries - remaining + 1) * retryBackoffMilliseconds);
            } catch (Exception e) {
                throw new RuntimeException(String.format(
                        "Unexpected issue while attempting to safely upsert to path %s: %s", path, e.getMessage()), e);
            }
        }

        throw new RuntimeException(String.format("Could not upsert to path %s after %s attempts", path, numRetries));
    }

    private <T> void createNewPathWithDoc(Function<T, T> upserter, Path path, ConfigurationController<?> controller)
            throws Exception {
        T docTyped = upserter.apply(null);
        if (docTyped == null) {
            throw new NullPointerException("Upserter must not return null");
        }
        Document docRaw = DocumentUtils.toRawDocument(docTyped);
        controller.create(path, docRaw);
    }

    private void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(String.format("Unexpected issue while attempting to safely upsert: %s",
                    e.getMessage()));
        }
    }

    private long retryBackoffMilliseconds;
    private int numRetries;
}
