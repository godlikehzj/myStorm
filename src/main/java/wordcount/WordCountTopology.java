package wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by godlikehzj on 2016/11/30.
 */
public class WordCountTopology {
    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentence-spout", new SentenceSpout(),2);
        builder.setBolt("split-bolt", new SplitSentenceBolt(),2).shuffleGrouping("sentence-spout");
        builder.setBolt("count-bolt", new WordCountBolt(),2).fieldsGrouping("split-bolt", new Fields("word"));

        Config config = new Config();
//        config.setNumAckers(0);
        config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();

        if (args.length  > 0){
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }else {
            cluster.submitTopology("wordCount", config, builder.createTopology());
        }

//        try {
//            Thread.sleep(10000);
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        cluster.killTopology("wordCount");
//        cluster.shutdown();
    }
}
