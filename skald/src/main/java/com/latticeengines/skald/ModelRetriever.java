package com.latticeengines.skald;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.skald.model.ModelIdentifier;

// Retrieves and caches the actual PMML files and their resultant expensive structures.
@Service
public class ModelRetriever {
    public ModelRetriever() {
        // TODO Make the cache properties tunable.
        cache = CacheBuilder.newBuilder().maximumSize(200)
                .build(new CacheLoader<AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>, ModelEvaluator>() {
                    @Override
                    public ModelEvaluator load(AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier> key)
                            throws Exception {
                        // TODO This path should be parameterized with the full
                        // space, not just the contract.

                        // TODO This path should be parameterized with the model
                        // version instead of random (and hard-coded) GUIDs.

                        URL address;
                        try {
                            String pattern = "http://%s/webhdfs/v1/user/s-analytics/customers/%s/models/%s/%s/rfpmml.xml?op=OPEN";
                            address = new URL(String.format(pattern, properties.getHdfsAddress(), key.getKey()
                                    .getContractId(), key.getValue().name,
                                    "b2d0c3f4-b767-4483-8c3c-f36d5cbe197d/1416355548818_22888"));
                        } catch (MalformedURLException ex) {
                            throw new RuntimeException("Failed to generate WebHDFS URL", ex);
                        }

                        log.info("Retrieving model from " + address.toString());
                        try (InputStreamReader reader = new InputStreamReader(address.openStream(),
                                StandardCharsets.UTF_8)) {
                            return new ModelEvaluator(reader);
                        } catch (IOException ex) {
                            throw new RuntimeException("Failed to read PMML file from WebHDFS", ex);
                        }
                    }
                });
    }

    public ModelEvaluator getEvaluator(CustomerSpace space, ModelIdentifier model) {
        return cache.getUnchecked(new AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>(space, model));
    }

    @Autowired
    private SkaldProperties properties;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>, ModelEvaluator> cache;

    private static final Log log = LogFactory.getLog(ModelRetriever.class);
}
