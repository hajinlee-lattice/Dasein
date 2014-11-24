package com.latticeengines.skald;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.skald.model.ScoreDerivation;

// Retrieves and caches the actual PMML files and their resultant expensive structures.
@Service
public class ModelRetriever {
    public ModelEvaluator getEvaluator(CustomerSpace space, String model, ScoreDerivation derivation) {
        // TODO Implement a caching layer.

        // TODO Create a lookup path for PMML files parameterized on just the
        // space and model name.

        // TODO Look in a properties directory to get the correct HDFS address.

        // TODO Build this URL in a (slightly) less terrible way.
        URL address;
        try {
            address = new URL(
                    String.format(
                            "http://%1$s:%2$d/webhdfs/v1/%3$s?op=OPEN",
                            "bodcdevvhort148.lattice.local",
                            50070,
                            "user/s-analytics/customers/Nutanix/models/Q_EventTable_Nutanix/0764821e-cceb-44fd-982e-d53b105d5c14/1414617158371_6972/rfpmml.xml"));
        } catch (MalformedURLException ex) {
            throw new RuntimeException("Failed to generate WebHDFS URL", ex);
        }

        log.info("Retrieving model from " + address.toString());
        try (InputStreamReader reader = new InputStreamReader(address.openStream(), StandardCharsets.UTF_8)) {
            return new ModelEvaluator(reader, derivation);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to read PMML file from WebHDFS", ex);
        }
    }

    private static final Log log = LogFactory.getLog(ModelRetriever.class);
}
