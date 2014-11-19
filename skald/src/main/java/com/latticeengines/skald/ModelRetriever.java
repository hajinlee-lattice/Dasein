package com.latticeengines.skald;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.xml.bind.JAXBException;

import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.jpmml.manager.PMMLManager;
import org.springframework.stereotype.Service;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

// Retrieves and caches the actual PMML files and their resultant expensive structures.
@Service
public class ModelRetriever {
    public PMMLManager getModel(CustomerSpace space, String model) {
        // TODO Replace with actual retrieval logic.
        String text;
        try {
            byte[] buffer = Files.readAllBytes(Paths.get("c:\\users\\wbaumann\\desktop\\rfpmml.xml"));
            text = new String(buffer, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read PMML file", e);
        }

        PMML pmml;
        try {
            pmml = IOUtil.unmarshal(new InputSource(new StringReader(text)));
        } catch (SAXException | JAXBException e) {
            throw new RuntimeException("Unable to parse PMML file", e);
        }

        PMMLManager manager = new PMMLManager(pmml);
        return manager;
    }
}
