package com.latticeengines.camille.exposed.interfaces.data;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class DataInterfaceSubscriber extends DataInterfaceBase {
    public DataInterfaceSubscriber(String interfaceName, CustomerSpace space) throws Exception {
        super(interfaceName, space);
    }

    /**
     * @return The corresponding Document, or null if no such Document exists.
     */
    public Document get(Path localPath) throws Exception {
        try {
            return CamilleEnvironment.getCamille().get(getBasePath().append(localPath));
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    public List<AbstractMap.SimpleEntry<Document, Path>> getChildren(Path localPath) throws Exception {
        List<AbstractMap.SimpleEntry<Document, Path>> children = CamilleEnvironment.getCamille().getChildren(
                getBasePath().append(localPath));

        if (children == null) {
            return Collections.emptyList();
        }

        List<AbstractMap.SimpleEntry<Document, Path>> out = new ArrayList<>(children.size());

        for (AbstractMap.SimpleEntry<Document, Path> child : children) {
            out.add(new AbstractMap.SimpleEntry<>(child.getKey(), child.getValue().local(getBasePath())));
        }

        return out;
    }
}
