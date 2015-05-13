package com.latticeengines.dellebi.routebuilder;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dellebi.util.MailSender;

public class FileArchiveRouteBuilder extends RouteBuilder {

	private static final Log log = LogFactory
			.getLog(FileArchiveRouteBuilder.class);

	@Value("${dellebi.cameldataincomepath}")
	private String camelDataIncomePath;
	@Value("${dellebi.cameldataarchivepath}")
	private String camelDataArchivePath;

	@Value("${dellebi.mailreceivelist}")
	private String mailReceiveList;

	@Value("${dellebi.inputfileregex}")
	private String inputFileRegex;

	@Autowired
	private MailSender mailSender;

	public void configure() {

		try {
			from(camelDataIncomePath)
					.choice()
					.when(header("CamelFileName").startsWith("tgt_quote_trans_global"))
						.process(new Processor() {
							public void process(Exchange exchange) throws Exception {
								log.info("Received Dell EBI file: "
										+ exchange.getIn().getHeader(
												"CamelFileName"));
	
								mailSender.sendEmail(mailReceiveList,"New Dell EBI files arrive!","Received Dell EBI file: "+ exchange.getIn().getHeader("CamelFileName") + " in " + System.getProperty("DELLEBI_PROPDIR") + " environment.");
							}
						})
						.multicast()
						.stopOnException()
						.to(camelDataArchivePath, "direct:files")
					.endChoice()
					.otherwise().stop()
					.end();
		} catch (Exception e) {
			log.info("File archiving failed!");
		}
	}
}
