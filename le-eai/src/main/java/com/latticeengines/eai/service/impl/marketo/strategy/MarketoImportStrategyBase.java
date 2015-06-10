package com.latticeengines.eai.service.impl.marketo.strategy;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;
import com.latticeengines.eai.service.impl.AvroTypeConverter;
import com.latticeengines.eai.service.impl.ImportStrategy;

@Component
public abstract class MarketoImportStrategyBase extends ImportStrategy {
    
    @Autowired
    private AvroTypeConverter marketoToAvroTypeConverter;

    public MarketoImportStrategyBase(String name) {
        super(name);
    }
    
    protected Map<String, Object> getHeaders(ImportContext ctx) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(MarketoImportProperty.IMPORTCONTEXT, ctx);
        properties.put(Exchange.CONTENT_TYPE, "application/json");
        return properties;
    }
    
    @Override
    protected AvroTypeConverter getAvroTypeConverter() {
        return marketoToAvroTypeConverter;
    }
    
    @Override
    public Table importMetadata(ProducerTemplate template, Table table, ImportContext ctx) {
        AvroTypeConverter converter = getAvroTypeConverter();
        List<Attribute> attributes = table.getAttributes();
        for (Attribute attribute : attributes) {
            assert(attribute != null);
            assert(converter != null);
            assert(converter.convertTypeToAvro(attribute.getLogicalDataType()) != null);
            
            if (attribute.getLogicalDataType() != null) {
                attribute.setPhysicalDataType(converter.convertTypeToAvro(attribute.getLogicalDataType()).name());
            }
        }
        return table;
    }
    
    @Override
    public ImportContext resolveFilterExpression(String expression, ImportContext ctx) {
        return ctx;
    }
    
    protected void setupPagingToken(ProducerTemplate template, ImportContext ctx, String sinceDateTime) {
        ImportStrategy pagingTokenStrategy = ImportStrategy.getImportStrategy(SourceType.MARKETO, "PagingToken");
        if (pagingTokenStrategy == null) {
            throw new RuntimeException("No paging token strategy.");
        }
        DateTimeFormatter dtf = ISODateTimeFormat.dateTimeParser();
        if (sinceDateTime == null) {
            DateTime today = new DateTime(new Date());
            sinceDateTime = today.minusYears(1).toString(dtf);
        } else {
            dtf.parseDateTime(sinceDateTime);
        }
        ctx.setProperty(MarketoImportProperty.SINCEDATETIME, sinceDateTime);
        pagingTokenStrategy.importData(template, null, null, ctx);
    }
    
    protected Map<String, Object> parse(String expression) {
        ExpressionParserVisitorBase exprVisitor = getParser();
        return parse(exprVisitor, expression);
    }
    
    protected ExpressionParserVisitorBase getParser() {
        return null;
    }
    
    protected static Map<String, Object> parse(ExpressionParserVisitorBase exprVisitor, String expression) {
        if (exprVisitor == null) {
            return new HashMap<>();
        }
        SQLParser parser = new SQLParser();
        try {
            StatementNode stmt = parser.parseStatement("SELECT * FROM T WHERE " + expression);
            stmt.accept(exprVisitor);
            return exprVisitor.getExpressions();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
