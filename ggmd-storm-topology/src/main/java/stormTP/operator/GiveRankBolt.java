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

public class GiveRankBolt extends BaseRichBolt {

    private OutputCollector collector;
    // map des positions (id -> nbCellsParcourus) pour le top courant
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

        // MyTortoiseBolt émet "nbCellsParcourus", "total" et "maxcel"
        int nbCells = t.getIntegerByField("nbCellsParcourus");
        positions.put(id, nbCells);

        // construire une liste triée des totaux (desc)
        List<Integer> sorted = new ArrayList<>(positions.values());
        Collections.sort(sorted, Collections.reverseOrder());

        // map total->rank (competition ranking: 1,2,2,4)
        Map<Integer, Integer> totalToRank = new LinkedHashMap<>();
        Map<Integer, Integer> freq = new HashMap<>();
        for (Integer v : sorted) freq.put(v, freq.getOrDefault(v, 0) + 1);

        for (int i = 0; i < sorted.size(); i++) {
            Integer val = sorted.get(i);
            if (!totalToRank.containsKey(val)) {
                totalToRank.put(val, i + 1);
            }
        }

        int myTotal = positions.get(id);
        int myRankNum = totalToRank.get(myTotal);
        String myRank = Integer.toString(myRankNum) + (freq.get(myTotal) > 1 ? "ex" : "");

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
