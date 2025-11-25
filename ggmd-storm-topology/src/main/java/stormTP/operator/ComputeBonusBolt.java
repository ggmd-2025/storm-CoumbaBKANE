package stormTP.operator;

import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.TopologyContext;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class ComputeBonusBolt extends BaseRichBolt {

    private OutputCollector collector;
    
    private Map<Integer, Integer> positions = new HashMap<>();
    private int currentTop = -1;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        int id = t.getIntegerByField("id");
        int top = t.getIntegerByField("top");

        // réinitialiser si on passe à un nouveau top
        if (top != currentTop) {
            currentTop = top;
            positions.clear();
        }

        
        int total = t.getIntegerByField("total");
        int maxcel = t.getIntegerByField("maxcel");

        collector.emit(new Values(
            id,
            top,
            myRank,
            total,
            maxcel
        ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new org.apache.storm.tuple.Fields(
            "id", "top", "rang", "total", "maxcel"
        ));
    }
}
