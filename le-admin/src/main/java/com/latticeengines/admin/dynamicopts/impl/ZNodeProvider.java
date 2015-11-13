package com.latticeengines.admin.dynamicopts.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooDefs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.dynamicopts.MutableOptionsProvider;
import com.latticeengines.admin.dynamicopts.OptionsProvider;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class ZNodeProvider implements OptionsProvider, MutableOptionsProvider {

    private final Path zNodePath;
    private Camille camille;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Log log = LogFactory.getLog(ZNodeProvider.class);

    public ZNodeProvider(Path path) {
        this.zNodePath = path;
        this.camille = CamilleEnvironment.getCamille();
    }

    @Override
    public List<String> getOptions() {
        try {
            String zNodeValue = this.camille.get(this.zNodePath).getData();
            JsonNode arrayNode = MAPPER.readTree(zNodeValue);
            List<String> options = new ArrayList<>();
            if (arrayNode.isArray()) {
                for (JsonNode node : arrayNode) {
                    options.add(node.asText());
                }
            }
            return options;
        } catch (Exception e) {
            log.error(String.format("Failed to convert ZNode %s to a String list.", this.zNodePath), e);
            return Collections.emptyList();
        }
    }

    @Override
    public void setOptions(List<String> options) {
        try {
            if (!this.camille.exists(this.zNodePath)) {
                this.camille.create(this.zNodePath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
            this.camille.set(this.zNodePath, new Document(MAPPER.writeValueAsString(options)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to set options for a ZNodeProvider at " + this.zNodePath.toString(), e);
        }
    }
}
