package com.latticeengines.skald;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
                        // TODO Create a lookup path for PMML files
                        // parameterized on just the space and model identifier.

                        // TODO Look in a properties directory to get the
                        // correct HDFS address.

                        // TODO Build this URL in a less terrible way.

                        URL address;
                        try {
                            address = new URL(
                                    String.format(
                                            "http://%1$s:%2$d/webhdfs/v1/%3$s?op=OPEN",
                                            "bodcdevvhort148.lattice.local",
                                            50070,
                                            "user/s-analytics/customers/Nutanix/models/Q_EventTable_Nutanix/68386ba1-0469-41ec-9663-ee7966dccf09/1414617158371_7241/rfpmml.xml"));
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

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>, ModelEvaluator> cache;

    private static final Log log = LogFactory.getLog(ModelRetriever.class);
}
