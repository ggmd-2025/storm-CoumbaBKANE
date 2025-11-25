package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.Exit3Bolt;

public class TopologyT3 {

    public static void main(String[] args) throws Exception {

        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        int nbExecutors = 2;

        // IMPORTANT : remplacer si Codespaces
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        // En cluster Docker : InputStreamSpout("storm-client", portINPUT)

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("masterStream", spout);

        builder.setBolt("filter", new GiveRankBolt(), nbExecutors)
               .shuffleGrouping("masterStream");

        builder.setBolt("exit2", new Exit3Bolt(), nbExecutors)
               .shuffleGrouping("filter");

        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);

        StormSubmitter.submitTopology("TopoT2", config, builder.createTopology());
    }
}
