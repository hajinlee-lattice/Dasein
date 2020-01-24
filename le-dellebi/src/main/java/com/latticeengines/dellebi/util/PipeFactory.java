package com.latticeengines.dellebi.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.dellebi.process.dailyrefresh.function.ScrubGeneralFunction;

import cascading.operation.AssertionLevel;
import cascading.operation.assertion.AssertSizeEquals;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public final class PipeFactory {

    protected PipeFactory() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(PipeFactory.class);

    public static Pipe getPipe(String pipeName, String fields, String exportedFields) {

        Pipe docPipe = null;

        if ("generic_item_Pipe".equals(pipeName)) {
            docPipe = createGenericItemPipe(fields, exportedFields);
        } else {
            log.error(pipeName + " is not registed!");
        }

        return docPipe;

    }

    private static Pipe createGenericItemPipe(String fields, String exportedFields) {

        log.info("Input fields: " + fields);
        log.info("Exported fields: " + exportedFields);

        Pipe docPipe;
        List<String> items = new ArrayList<String>(Arrays.asList(fields.split(",")));
        int sizeOfFields = items.size();

        Fields scrubArguments = new Fields(items.get(0));
        items.remove(0);

        for (String s : items) {
            Fields scrubArgument = new Fields(s);
            scrubArguments = scrubArguments.append(scrubArgument);
        }

        List<String> exportedItems = new ArrayList<String>(Arrays.asList(exportedFields.split(",")));

        Fields outputScrubArguments = new Fields(exportedItems.get(0));
        exportedItems.remove(0);

        for (String s : exportedItems) {
            Fields outputScrubrgument = new Fields(s);
            outputScrubArguments = outputScrubArguments.append(outputScrubrgument);
        }

        docPipe = new Pipe("copy");
        AssertSizeEquals equals = new AssertSizeEquals(sizeOfFields);
        docPipe = new Each(docPipe, AssertionLevel.VALID, equals);
        docPipe = new Each(docPipe, scrubArguments, new ScrubGeneralFunction(outputScrubArguments), Fields.RESULTS);
        return docPipe;
    }
}
