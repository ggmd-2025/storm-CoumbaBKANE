package stormTP.operator;

import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public class MyTortoiseBolt extends BaseRichBolt {

    private OutputCollector collector;
    private int idTortueChoisie = 3;   
    private String name = "Caroline"; 
    private int lastCell = 0;
    private int nbCellsParcourus = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        int id = t.getIntegerByField("id");

        if (id == idTortueChoisie) {
            int cell = t.getIntegerByField("cellule");
            int tour = t.getIntegerByField("tour");

            // calcul du nombre total de cellules parcourues par la tortue
            nbCellsParcourus = tour * t.getIntegerByField("maxcel") + cell;

            collector.emit(new Values(
                id,
                t.getIntegerByField("top"),
                name,
                nbCellsParcourus,
                t.getIntegerByField("total"),
                t.getIntegerByField("maxcel")
            ));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new org.apache.storm.tuple.Fields(
            "id", "top", "nom", "nbCellsParcourus", "total", "maxcel"
        ));
    }
}
