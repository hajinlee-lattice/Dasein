package com.latticeengines.db.exposed.schemagen.postprocess;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

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
            try (InputStream stream = new FileInputStream(file)) {
                List<String> lines = IOUtils.readLines(stream);
                process(lines);
                try (OutputStream ostream = new FileOutputStream(file)) {
                    IOUtils.writeLines(lines, "\n", ostream);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void process(List<String> input) {
        ListIterator<String> iter = input.listIterator();
        while (iter.hasNext()) {
            String next = iter.next();
            for (LineProcessor processor : lineProcessors) {
                List<String> output = processor.processLine(next);
                if (output.size() == 0) {
                    iter.remove();
                } else {
                    iter.set(output.get(0));
                }
                for (int i = 1; i < output.size(); ++i) {
                    iter.add(output.get(i));
                }
            }
        }
    }
}
