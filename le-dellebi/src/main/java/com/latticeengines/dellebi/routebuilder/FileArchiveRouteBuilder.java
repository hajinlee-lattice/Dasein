package com.latticeengines.dellebi.routebuilder;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dellebi.util.HadoopFileSystemOperations;
import com.latticeengines.dellebi.util.MailSender;

@Deprecated
public class FileArchiveRouteBuilder extends RouteBuilder {

    private static final Log log = LogFactory.getLog(FileArchiveRouteBuilder.class);

    @Value("${dellebi.cameldataincomepath}")
    private String camelDataIncomePath;

    @Value("${dellebi.cameldataarchivepath}")
    private String camelDataArchivePath;

    @Value("${dellebi.datahadoopinpath}")
    private String dataHadoopInpath;

    @Value("${dellebi.mailreceivelist}")
    private String mailReceiveList;

    @Value("${dellebi.inputfileregex}")
    private String inputFileRegex;

    @Value("${dellebi.env}")
    private String env;

    @Value("${dellebi.quotetrans}")
    private String quoteTrans;

    @Autowired
    private MailSender mailSender;

    @Autowired
    private HadoopFileSystemOperations hadoopfilesystemoperations;

    public void configure() {
        try {
            from(camelDataIncomePath)
                    .process(new Processor() {
                        public void process(Exchange exchange) throws Exception {
                            while (hadoopfilesystemoperations
                                    .isExistWithTXTFile(dataHadoopInpath + "/"
                                            + quoteTrans)) {
                                try {
                                    Thread.sleep(6000);
                                } catch (InterruptedException e) {
                                }
                            }
                        }
                    })
                    .choice()
                    .when(header("CamelFileName").startsWith(
                            "tgt_quote_trans_global"))
                    .process(new Processor() {
                        public void process(Exchange exchange) throws Exception {
                            log.info("Received Dell EBI file: "
                                    + exchange.getIn().getHeader(
                                            "CamelFileName"));

                            mailSender.sendEmail(
                                    mailReceiveList,
                                    "New Dell EBI files arrive!",
                                    "Received Dell EBI file: "
                                            + exchange.getIn().getHeader(
                                                    "CamelFileName") + " in "
                                            + env + " environment.");
                        }
                    }).multicast().stopOnException()
                    .to("direct:files").endChoice()
                    .otherwise().stop().end();
        } catch (Exception e) {
            log.info("File archiving failed!");
        }
    }
}
