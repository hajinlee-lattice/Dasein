package com.latticeengines.camille.paths;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class FileSystemToZooKeeperFunction implements Function<Path, List<Map.Entry<Document, Path>>> {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    // the exposed directory
    private String baseDir;

    public FileSystemToZooKeeperFunction(String baseDir) {
        // clip the slash at the end if it exists
        if (baseDir == null || baseDir.isEmpty())
            this.baseDir = "";
        else {
            char last = baseDir.charAt(baseDir.length() - 1);
            this.baseDir = last == '\\' || last == '/' ? baseDir.substring(0, baseDir.length() - 1) : baseDir;
        }
    }

    private String toAbsolutePath(Path p) {
        StringBuilder sb = new StringBuilder(baseDir);
        for (String part : p.getParts())
            sb.append("\\").append(part);
        return sb.toString();
    }

    @Override
    public List<Entry<Document, Path>> apply(Path parentPath) {
        String[] children = new File(toAbsolutePath(parentPath)).list();
        if (children == null)
            return new ArrayList<Map.Entry<Document, Path>>(0);
        List<Map.Entry<Document, Path>> out = new ArrayList<Map.Entry<Document, Path>>(children.length);
        for (String relChildPathStr : children) {
            Path childPath = parentPath.append(relChildPathStr);
            String absoluteChildPathStr = toAbsolutePath(childPath);
            File f = new File(absoluteChildPathStr);
            Document doc;
            try {
                doc = f.isDirectory() ? new Document() : new Document(new String(Files.readAllBytes(Paths
                        .get(absoluteChildPathStr))));
            } catch (IOException e) {
                doc = null;
                log.error("Error reading " + childPath, e);
            }
            out.add(Pair.of(doc, childPath));
        }
        return out;
    }
}