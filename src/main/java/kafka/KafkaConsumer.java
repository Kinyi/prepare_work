package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread{
	private String topic;

	public KafkaConsumer(String topic) {
		super();
		this.topic = topic;
	}
	
	@Override
	public void run() {
		ConsumerConnector consumerConnector = createConsumer();
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>(); 
		topicCountMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> kafkaStream = messageStreams.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println(message);
		}
	}

	private ConsumerConnector createConsumer() {
		Properties prop = new Properties();
		prop.put("zookeeper.connect","192.168.80.100:2181,192.168.80.101:2181,192.168.80.102:2181");
		prop.put("group.id", "group1");
		
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
	}
	
	public static void main(String[] args) {
		new KafkaConsumer("test").start();
	}
}