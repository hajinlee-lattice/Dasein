package com.latticeengines.dellebi.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.operation.AssertionLevel;
import cascading.operation.assertion.AssertSizeEquals;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.latticeengines.dellebi.process.dailyrefresh.function.ScrubOdrDtlFunction;
import com.latticeengines.dellebi.process.dailyrefresh.function.ScrubOdrDtlFunctionReplace;
import com.latticeengines.dellebi.process.dailyrefresh.function.ScrubOdrSumFunction;
import com.latticeengines.dellebi.process.dailyrefresh.function.ScrubOdrSumFunctionReplace;
import com.latticeengines.dellebi.process.dailyrefresh.function.ScrubQuoteFunction;
import com.latticeengines.dellebi.process.dailyrefresh.function.ScrubShipAddrFunction;
import com.latticeengines.dellebi.process.dailyrefresh.function.ScrubWarFunction;

public class PipeFactory {

    private static final Log log = LogFactory.getLog(PipeFactory.class);

    public static Pipe getPipe(String pipeName, String fields) {

        Pipe docPipe = null;

        switch (pipeName) {
        case "order_detail_Pipe":
            docPipe = createOdrDetialPipe(fields);
            break;
        case "order_summary_Pipe":
            docPipe = createOdrSumPipe(fields);
            break;
        case "ship_to_addr_lattice_Pipe":
            docPipe = createShipAddrPipe(fields);
            break;
        case "warranty_global_Pipe":
            docPipe = createWarrantyPipe(fields);
            break;
        case "quote_trans_Pipe":
            docPipe = createQuoteTransPipe(fields);
            break;
        default:
            log.error(pipeName + " is not registed!");
        }

        return docPipe;

    }

    private static Pipe createWarrantyPipe(String fields) {
        Pipe docPipe;
        Fields scrubArguments = new Fields("#ORDER_BUSINESS_UNIT_ID");

        List<String> items = new ArrayList<String>(Arrays.asList(fields.split(",")));
        for (String s : items) {
            Fields scrubArgument = new Fields(s);
            scrubArguments = scrubArguments.append(scrubArgument);
        }

        // There're 2 new fields should be added to output file:
        // PROCESSED_FLG is 0; STAGE_DT is current date.
        Fields outputScrubArguments = scrubArguments.append(new Fields("PROCESSED_FLG")).append(new Fields("STAGE_DT"))
                .append(new Fields("FileName"));

        docPipe = new Pipe("copy");
        docPipe = new Each(docPipe, scrubArguments, new ScrubWarFunction(outputScrubArguments), Fields.RESULTS);
        return docPipe;
    }

    private static Pipe createShipAddrPipe(String fields) {
        Pipe docPipe;
        Fields scrubArguments = new Fields("#ORD_NBR");

        List<String> items = new ArrayList<String>(Arrays.asList(fields.split(",")));
        for (String s : items) {
            Fields scrubArgument = new Fields(s);
            scrubArguments = scrubArguments.append(scrubArgument);
        }

        // There're 2 new fields should be added to output file:
        // PROCESSED_FLG is 0; STAGE_DT is current date.
        Fields outputScrubArguments = scrubArguments.append(new Fields("PROCESSED_FLG")).append(new Fields("STAGE_DT"))
                .append(new Fields("FileName"));

        docPipe = new Pipe("copy");
        docPipe = new Each(docPipe, scrubArguments, new ScrubShipAddrFunction(outputScrubArguments), Fields.RESULTS);
        return docPipe;
    }

    private static Pipe createOdrSumPipe(String fields) {
        Pipe docPipe;
        Fields scrubArguments = new Fields("#ORD_NBR");

        List<String> items = new ArrayList<String>(Arrays.asList(fields.split(",")));
        for (String s : items) {
            Fields scrubArgument = new Fields(s);
            scrubArguments = scrubArguments.append(scrubArgument);
        }

        // There're 3 new fields should be added to output file:
        // PROCESSED_FLG is 0; STAGE_DT is current date;FileName is input file
        // name.
        Fields outputScrubArguments = scrubArguments.append(new Fields("PROCESSED_FLG")).append(new Fields("STAGE_DT"))
                .append(new Fields("FileName"));

        docPipe = new Pipe("copy");
        docPipe = new Each(docPipe, scrubArguments, new ScrubOdrSumFunction(outputScrubArguments), Fields.RESULTS);

        Fields replaceScrubArgument = new Fields("SRC_LCL_CHNL_CD", "REF_LCL_CHNL_CD", "CNCL_DT", "INV_DT", "ORD_DT",
                "ORD_STAT_DT", "SHIP_DT", "EXCH_DT", "SHIP_BY_DT", "PRF_OF_DLVR_DT", "ESTD_BUS_DLVR_DT");

        docPipe = new Each(docPipe, replaceScrubArgument, new ScrubOdrSumFunctionReplace(replaceScrubArgument),
                Fields.REPLACE);
        return docPipe;
    }

    private static Pipe createOdrDetialPipe(String fields) {
        Pipe docPipe;
        Fields scrubArguments = new Fields("#ORD_NUM");

        List<String> items = new ArrayList<String>(Arrays.asList(fields.split(",")));
        for (String s : items) {
            Fields scrubArgument = new Fields(s);
            scrubArguments = scrubArguments.append(scrubArgument);
        }

        // There're 2 new fields should be added to output file:
        // PROCESSED_FLG is 0; STAGE_DT is current date.
        Fields outputScrubArguments = scrubArguments.append(new Fields("PROCESSED_FLG")).append(new Fields("STAGE_DT"))
                .append(new Fields("FileName"));

        docPipe = new Pipe("copy");
        docPipe = new Each(docPipe, scrubArguments, new ScrubOdrDtlFunction(outputScrubArguments), Fields.RESULTS);

        Fields replaceScrubArgument = new Fields("SVC_TAG_ID", "SRC_BU_ID");

        docPipe = new Each(docPipe, replaceScrubArgument, new ScrubOdrDtlFunctionReplace(replaceScrubArgument),
                Fields.REPLACE);
        return docPipe;
    }

    private static Pipe createQuoteTransPipe(String fields) {

        log.info("Create quote trans pipe!");
        log.info("Quote fields: " + fields);

        Pipe docPipe;
        Fields scrubArguments = new Fields("#QTE_NUM_VAL");

        List<String> items = new ArrayList<String>(Arrays.asList(fields.split(",")));
        for (String s : items) {
            Fields scrubArgument = new Fields(s);
            scrubArguments = scrubArguments.append(scrubArgument);
        }
        Fields outputScrubArguments = new Fields("#QTE_NUM_VAL").append(new Fields("QUOTE_CREATE_DATE"))
                .append(new Fields("SLDT_CUST_NUM_VAL")).append(new Fields("ITM_NUM_VAL"))
                .append(new Fields("LEAD_SLS_REP_ASSOC_BDGE_NBR")).append(new Fields("SYS_QTY"))
                .append(new Fields("REVN_USD_AMT")).append(new Fields("SLDT_BU_ID"))
                .append(new Fields("fileName"));

        docPipe = new Pipe("copy");
        AssertSizeEquals equals = new AssertSizeEquals(111);
        docPipe = new Each(docPipe, AssertionLevel.VALID, equals);
        docPipe = new Each(docPipe, scrubArguments, new ScrubQuoteFunction(outputScrubArguments), Fields.RESULTS);
        return docPipe;
    }
}
