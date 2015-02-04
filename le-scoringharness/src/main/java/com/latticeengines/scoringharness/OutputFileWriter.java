package com.latticeengines.scoringharness;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OutputFileWriter implements AutoCloseable {
    public static class Result {
        public Result() {
            additionalFields = new ArrayList<String>();
        }

        public long offsetMilliseconds;
        public String operation;
        public boolean isSuccess;
        public List<String> additionalFields;
    }

    private FileWriter writer;
    private String path;

    public OutputFileWriter(String path) throws IOException {
        this.path = path;
        this.writer = new FileWriter(path);
    }

    @Override
    public void close() throws Exception {
        if (writer != null)
            writer.close();
    }

    public void write(Result r) {
        try {
            writer.write(new Long(r.offsetMilliseconds).toString());
            writer.write("\t");
            writer.write(r.operation != null ? r.operation : "");
            writer.write("\t");
            writer.write((r.isSuccess ? "SUCCESS" : "FAILURE"));
            writer.write("\t");

            if (r.additionalFields != null) {
                for (int i = 0; i < r.additionalFields.size(); ++i) {
                    writer.write(r.additionalFields.get(i));
                    if (i != r.additionalFields.size() - 1) {
                        writer.write("\t");
                    }
                }
            }
            writer.write(String.format("%n"));
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to file " + path, e);
        }
    }
}
