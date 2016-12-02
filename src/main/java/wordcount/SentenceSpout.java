package wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by godlikehzj on 2016/11/29.
 */
public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] sentences={
            "ppp vv",
            "vv bb",
            "sss iii kkk",
            "bbb ddd ooo"
    };
    private int index = 0;
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
       declarer.declare(new Fields("sentence"));
    }

    public void nextTuple(){
        this.collector.emit(new Values(sentences[index]));
        System.out.println(Thread.currentThread().getName() + " -- create spout :" + sentences[index]);
        index++;
        if (index >= sentences.length){
            index = 0;
        }
        try{
            Thread.sleep(1000);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
