package kafka;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class KafkaProducer extends Thread {
	String topic;

	public KafkaProducer(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		Producer<Integer, String> producer = createProducer();

		int i = 0;
		while (true) {
			String message = "message" + i++;
			producer.send(new KeyedMessage<Integer, String>(topic, message));
			
			System.out.println("发送"+message);
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Producer<Integer, String> createProducer() {
		Properties prop = new Properties();
		prop.put("zookeeper.connect","192.168.80.100:2181,192.168.80.101:2181,192.168.80.102:2181");
		prop.put("serializer.class", StringEncoder.class.getName());
		prop.put("metadata.broker.list","192.168.80.100:9092,192.168.80.101:9092,192.168.80.102:9092");
		return new Producer<Integer, String>(new ProducerConfig(prop));
	}

	public static void main(String[] args) {
		new KafkaProducer("test").start();
	}
}