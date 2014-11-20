package com.latticeengines.skald;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.skald.model.ScoreDerivation;

// Retrieves and caches the actual PMML files and their resultant expensive structures.
@Service
public class ModelRetriever {
    public ModelEvaluator getEvaluator(CustomerSpace space, String model, ScoreDerivation derivation) {
        // TODO Replace with actual retrieval logic.
        String text;
        try {
            byte[] buffer = Files.readAllBytes(Paths.get("c:\\users\\wbaumann\\desktop\\rfpmml.xml"));
            text = new String(buffer, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read PMML file", e);
        }

        // TODO Implement a caching layer.
        return new ModelEvaluator(text, derivation);
    }
}
