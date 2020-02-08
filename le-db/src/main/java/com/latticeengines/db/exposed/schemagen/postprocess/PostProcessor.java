package com.latticeengines.db.exposed.schemagen.postprocess;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

public abstract class PostProcessor {
    private List<LineProcessor> lineProcessors = new ArrayList<>();

    public PostProcessor() {
    }

    protected void registerInnerProcessor(LineProcessor lineProcessor) {
        lineProcessors.add(lineProcessor);
    }

    protected interface LineProcessor {
        List<String> processLine(String line);
    }

    public void process(File file) {
        if (file.exists()) {
            Iterable<String> it = () -> {
                try {
                    return FileUtils.lineIterator(file, Charset.defaultCharset().name());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            File fout = new File(file.getAbsolutePath() + ".new");
            try (OutputStream ostream = new FileOutputStream(fout)) {
                for (String ln: it) {
                    List<String> lns = processLine(ln);
                    if (CollectionUtils.isNotEmpty(lns)) {
                        for (String ln2: lns) {
                            IOUtils.write(ln2 + "\n", ostream, Charset.defaultCharset());
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            FileUtils.deleteQuietly(file);
            try {
                FileUtils.moveFile(fout, file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<String> processLine(String ln) {
        List<String> lst = Collections.singletonList(ln);
        for (LineProcessor processor : lineProcessors) {
            List<String> lst2 = new ArrayList<>();
            for (String next: lst) {
                lst2.addAll(processor.processLine(next));
            }
            lst = lst2;
        }
        return lst;
    }
}
