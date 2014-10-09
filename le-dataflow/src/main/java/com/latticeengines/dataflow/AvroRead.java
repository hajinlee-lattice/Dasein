package com.latticeengines.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.latticeengines.common.exposed.util.AvroUtils;

public class AvroRead {

    public static void main(String[] args) throws Exception {
        String lead = "/tmp/Lead/Lead_09-10-2014.avro";
        String opportunity = "/tmp/Opportunity/Opportunity_09-10-2014.avro";
        String wcPath = "/tmp/b.txt";

        // Get the schema from a file
        Schema leadSchema = AvroUtils.getSchema(new Configuration(), new Path(lead));
        Schema opportunitySchema = AvroUtils.getSchema(new Configuration(), new Path(opportunity));

        Properties properties = new Properties();
        properties.put("mapred.job.queue.name", "Priority0.MapReduce.0");
        AppProps.setApplicationJarClass(properties, AvroRead.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        Map<String, Tap> sources = new HashMap<>();
        Tap<?, ?, ?> leadTap = new Hfs(new AvroScheme(), lead);
        Tap<?, ?, ?> opportunityTap = new Hfs(new AvroScheme(), opportunity);
        Tap<?, ?, ?> sink = new Hfs(new TextDelimited(), wcPath, true);
        sources.put("lhs", leadTap);
        sources.put("rhs", opportunityTap);

        List<String> fieldNames = new ArrayList<>();
        Set<String> seenFieldNames = new HashSet<>();

        for (Field field : leadSchema.getFields()) {
            String name = field.name();
            if (seenFieldNames.contains(name)) {
                name = name + "_1";
            }
            fieldNames.add(name);
            seenFieldNames.add(name);
        }

        for (Field field : opportunitySchema.getFields()) {
            String name = field.name();
            if (seenFieldNames.contains(name)) {
                name = name + "_1";
            }
            fieldNames.add(name);
            seenFieldNames.add(name);
        }
        String[] declaredFields = new String[fieldNames.size()];
        fieldNames.toArray(declaredFields);
        Pipe join = new CoGroup(new Pipe("lhs"), //
                new Fields("ConvertedOpportunityId"), //
                new Pipe("rhs"), //
                new Fields("Id"), //
                new Fields(declaredFields), //
                new InnerJoin());

        join = new GroupBy(join, new Fields("Email"));
        join = new Every(join, Fields.ALL, new Count(new Fields("countcount")), Fields.ALL);

        FlowDef flowDef = FlowDef.flowDef().setName("wc") //
                .addSources(sources) //
                .addTailSink(join, sink);
        // write a DOT file and run the flow
        Flow<?> flow = flowConnector.connect(flowDef);
        flow.writeDOT("dot/wcr.dot");
        flow.complete();
    }

}
