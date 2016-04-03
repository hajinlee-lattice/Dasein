package com.latticeengines.eai.routes.vdb.strategy;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.impl.AvroTypeConverter;
import com.latticeengines.eai.service.impl.ImportStrategy;

@Component("vdbTableImportStrategyBase")
public class VdbTableImportStrategyBase extends ImportStrategy {

	@SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(VdbTableImportStrategyBase.class);
	
	public VdbTableImportStrategyBase() {
	    super("Vdb.Table");
	}

	@Override
	public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
	}

	@Override
	public Table importMetadata(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
		return null;
	}

	@Override
	public ImportContext resolveFilterExpression(String expression, ImportContext ctx) {
		return null;
	}

	@Override
	protected AvroTypeConverter getAvroTypeConverter() {
		return null;
	}

}
