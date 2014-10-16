package com.latticeengines.camille.paths;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
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

    public FileSystemToZooKeeperFunction(File baseDir) throws IOException {
        switch (baseDir.getName()) {
        case "":
        case ".":
            try {
                this.baseDir = new java.io.File(".").getCanonicalPath();
            } catch (IOException e) {
                log.error("Error getting working directory", e);
                throw e;
            }
            break;
        default:
            if (!baseDir.isDirectory())
                throw new IllegalArgumentException("not a directory");
            this.baseDir = baseDir.getAbsolutePath().toString();
            break;
        }

        while (StringUtils.endsWithAny(this.baseDir, "\\", "/")) {
            this.baseDir = StringUtils.chop(this.baseDir);
        }
    }

    private String toAbsolutePath(Path p) {
        StringBuilder sb = new StringBuilder(baseDir);
        for (String part : p.getParts())
            sb.append('/').append(part);
        return sb.toString();
    }

    @Override
    public List<Entry<Document, Path>> apply(Path parentPath) {
        String[] children = new File(toAbsolutePath(parentPath)).list();
        if (children == null)
            return new ArrayList<Map.Entry<Document, Path>>(0);
        List<Map.Entry<Document, Path>> out = new ArrayList<Map.Entry<Document, Path>>(children.length);
        for (String relChildPathStr : children) {
            Path childPath = null;
            try {
                childPath = parentPath.append(relChildPathStr);
            } catch (IllegalArgumentException e) {
                log.error("Error reading " + relChildPathStr, e);
                continue;
            }

            String absoluteChildPathStr = toAbsolutePath(childPath);
            Document doc;
            if (new File(absoluteChildPathStr).isDirectory())
                doc = new Document();
            else {
                try {
                    doc = new Document(new String(Files.readAllBytes(Paths.get(absoluteChildPathStr))));
                } catch (IOException e) {
                    log.error("Error reading " + relChildPathStr, e);
                    continue;
                }
            }
            out.add(Pair.of(doc, childPath));
        }
        return out;
    }
}