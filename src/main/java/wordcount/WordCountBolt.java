package wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by godlikehzj on 2016/11/30.
 */
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Long> counts;

    public void prepare(Map config, TopologyContext context, OutputCollector collector){
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple){
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if (count == null){
            count = 0L;
        }
        count ++;
        this.counts.put(word, count);
        this.collector.emit(new Values(word, count));
        System.out.println(Thread.currentThread().getName() + "--" + word + " : " + count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
//        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup(){
        System.out.println("-------result--------");
        Iterator entries = this.counts.entrySet().iterator();
        while (entries.hasNext()){
            Map.Entry entry = (Map.Entry)entries.next();
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        System.out.println("---------end---------");
    }
}
