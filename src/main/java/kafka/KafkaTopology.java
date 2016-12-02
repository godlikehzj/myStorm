package kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.Properties;

/**
 * Created by godlikehzj on 2016/11/30.
 */
public class KafkaTopology {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopology.class);
    private static final String TOPIC_NAME = "mytest";
    private static final String ZK_ROOTS = "/kafka";
    private static final String ZK_ID = "wordCount";
    private static final String ZK_HOST = "127.0.0.1:2181/kafka";
    private static final String KAFKA_BROKER = "127.0.0.1:9092";

    public static void main(String[] args) throws Exception{
        ZkHosts zkHosts = new ZkHosts(ZK_HOST);
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, TOPIC_NAME, ZK_ROOTS, ZK_ID);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        Config conf = new Config();
        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_BROKER);
        props.put("producer.type", "async");
        props.put("request.required.acks", "0"); // 0 ,-1 ,1
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        KafkaBolt kafkaBolt = new KafkaBolt().withTopicSelector(new DefaultTopicSelector("test"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("test", "word"));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaTopicSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("splitSentenceBolt", new SplitSentenceBolt(), 4).shuffleGrouping("kafkaTopicSpout");
        builder.setBolt("kafkaWriteBolt", kafkaBolt, 4).shuffleGrouping("splitSentenceBolt");

        if (args.length > 0){
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCount", conf, builder.createTopology());
        }

    }
}
