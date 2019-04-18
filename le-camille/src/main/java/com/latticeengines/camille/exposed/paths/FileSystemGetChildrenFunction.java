package com.latticeengines.camille.exposed.paths;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.google.common.base.Function;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class FileSystemGetChildrenFunction implements Function<Path, List<Map.Entry<Document, Path>>> {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private String fileSystemDirectoryOfRootPathStr;

    public FileSystemGetChildrenFunction(File fileSystemDirectoryOfRootPath) throws IOException {
        switch (fileSystemDirectoryOfRootPath.getName()) {
        case "":
        case ".":
            try {
                fileSystemDirectoryOfRootPathStr = new java.io.File(".").getCanonicalPath();
            } catch (IOException e) {
                log.error("Error getting working directory", e);
                throw e;
            }
            break;
        default:
            if (!fileSystemDirectoryOfRootPath.isDirectory()) {
                IllegalArgumentException e = new IllegalArgumentException(MessageFormatter.format(
                        "{} is not a directory", fileSystemDirectoryOfRootPath).getMessage());
                log.error(e.getMessage(), e);
                throw e;
            }
            fileSystemDirectoryOfRootPathStr = fileSystemDirectoryOfRootPath.getAbsolutePath().toString();
            while (StringUtils.endsWithAny(this.fileSystemDirectoryOfRootPathStr, new String[] { "\\", "/" }))
                fileSystemDirectoryOfRootPathStr = StringUtils.chop(fileSystemDirectoryOfRootPathStr);
            break;
        }
    }

    private String toAbsolutePath(Path p) {
        StringBuilder sb = new StringBuilder(fileSystemDirectoryOfRootPathStr);
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
                log.warn(
                        MessageFormatter.format("File {} is in an invalid path format.", relChildPathStr).getMessage());
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
                    log.error(MessageFormatter.format("Cannot read {}", relChildPathStr).getMessage(), e);
                    continue;
                }
            }
            out.add(new AbstractMap.SimpleEntry<Document, Path>(doc, childPath));
        }
        return out;
    }
}
