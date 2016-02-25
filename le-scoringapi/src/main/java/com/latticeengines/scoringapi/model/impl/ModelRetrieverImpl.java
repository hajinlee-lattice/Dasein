package com.latticeengines.scoringapi.model.impl;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.ModelIdentifier;
import com.latticeengines.scoringapi.controller.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelType;
import com.latticeengines.scoringapi.infrastructure.ScoringProperties;
import com.latticeengines.scoringapi.model.ModelEvaluator;
import com.latticeengines.scoringapi.model.ModelRetriever;

// Retrieves and caches the actual PMML files and their resultant expensive structures.
@Component("modelRetriever")
public class ModelRetrieverImpl implements ModelRetriever {

    private static final Log log = LogFactory.getLog(ModelRetrieverImpl.class);

    @Value("${scoringapi.pls.api.hostport}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    private ScoringProperties properties;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>, ModelEvaluator> cache;

    public ModelRetrieverImpl() {
        // TODO Make the cache properties tunable.
        cache = CacheBuilder.newBuilder().maximumSize(200)
                .build(new CacheLoader<AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>, ModelEvaluator>() {
                    @Override
                    public ModelEvaluator load(AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier> key)
                            throws Exception {
                        URL address;
                        try {
                            String pattern = "http://%s/webhdfs/v1/user/s-analytics/customers/%s/models/%s/%s/ModelPmml.xml?op=OPEN";
                            address = new URL(String.format(pattern, properties.getHdfsAddress(), key.getKey(),
                                    key.getValue().name, key.getValue().version));
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

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public ModelEvaluator getEvaluator(CustomerSpace space, ModelIdentifier model) {
        return cache.getUnchecked(new AbstractMap.SimpleEntry<CustomerSpace, ModelIdentifier>(space, model));
    }

    @Override
    public List<Model> getActiveModels(String tenantId, ModelType type) {
        List<Model> models = new ArrayList<>();

        List<ModelSummary> modelSummaries = internalResourceRestApiProxy.getActiveModelSummaries(tenantId);
        for (ModelSummary modelSummary : modelSummaries) {
            Model model = new Model(modelSummary.getId(), modelSummary.getName(), type);
            models.add(model);
        }

        return models;
    }

    @Override
    public Fields getModelFields(String modelId) {
        // TODO Auto-generated method stub
        return null;
    }

}
