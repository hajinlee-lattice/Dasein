package com.latticeengines.dellebi.routebuilder;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dellebi.util.MailSender;

public class FileArchiveRouteBuilder extends RouteBuilder {

    private final static Logger LOGGER = Logger.getLogger(FileArchiveRouteBuilder.class);

    @Value("${dellebi.cameldataincomepath}")
    private String camelDataIncomePath;
    @Value("${dellebi.cameldataarchivepath}")
    private String camelDataArchivePath;
    
    @Value("${dellebi.mailreceivelist}")
    private String mailReceiveList;
    
    @Autowired
    private MailSender mailSender;

    public void configure() {

        try{
            from(camelDataIncomePath)
            .filter(header("CamelFileName").regex("tgt_(ship_to_addr_lattice|order_detail|lat_order_summary|warranty_global|quote_trans_global).+(.zip)"))
            .process(new Processor() {
            public void process(Exchange exchange) throws Exception {
                LOGGER.info("Received Dell EBI file: " + exchange.getIn().getHeader("CamelFileName"));

                mailSender.sendEmail(mailReceiveList,"New Dell EBI files arrive!","Received Dell EBI file: " + exchange.getIn().getHeader("CamelFileName"));
                }
            })
            .multicast().stopOnException().to(camelDataArchivePath, "direct:files").end();
        }catch(Exception e){
            LOGGER.info("File archiving failed!");
        }
    }
}
