package kafka;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by godlikehzj on 2016/11/30.
 */
public class SplitSentenceBolt extends BaseRichBolt{
    private static final Logger logger = LoggerFactory.getLogger(SplitSentenceBolt.class);
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext context, OutputCollector collector){
        this.collector = collector;
    }

    public void execute(Tuple tuple){
        String sentence = tuple.getString(0);
        logger.info("get kafka message "+sentence);
        String[] words = sentence.split(" ");
        for (String word : words) {
            List<Tuple> list = new ArrayList<Tuple>();
            list.add(tuple);
            collector.emit(list, new Values(word.toLowerCase()));
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
