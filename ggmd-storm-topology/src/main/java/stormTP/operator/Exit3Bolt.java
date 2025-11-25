package stormTP.operator;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;

import javax.json.JsonObject;
import javax.json.Json;
import java.util.Map;

public class Exit3Bolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {

        JsonObject json = Json.createObjectBuilder()
            .add("id", t.getIntegerByField("id"))
            .add("top", t.getIntegerByField("top"))
            .add("rang", t.getIntegerByField("rang"))
            .add("total", t.getIntegerByField("total"))
            .add("maxcel", t.getIntegerByField("maxcel"))
            .build();

        collector.emit(new Values(json.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new org.apache.storm.tuple.Fields("json"));
    }
}





