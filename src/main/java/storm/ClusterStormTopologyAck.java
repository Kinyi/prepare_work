package storm;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.utils.threadsafe;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ClusterStormTopologyAck {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("input", new DataSourceSpout());
		builder.setBolt("sum", new SumBolt(), 3).allGrouping("input");

		Config config = new Config();
		try {
			StormSubmitter.submitTopology(
					ClusterStormTopologyAck.class.getSimpleName(), config,
					builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class DataSourceSpout extends BaseRichSpout {
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;

		/**
		 * 在本实例运行时，首先被调用
		 */
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}

		/**
		 * 认为是heartbeat，永无休息，死循环的调用。线程安全的操作。
		 */
		int i = 0;
		AtomicInteger atomicInteger = new AtomicInteger(10);

		public void nextTuple() {
			// 送出去，送给bolt
			// Values是一个value的List
			System.err.println("spout:" + i);
			collector.emit(new Values(i++), atomicInteger.getAndAdd(1));
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// Fields是一个field的List
			declarer.declare(new Fields("v1"));
		}

		/**
		 * 当后继者bolt发送ack时，这里的ack方法会被调用
		 */
		@Override
		public void ack(Object msgId) {
			super.ack(msgId);
			System.out.println("调用了ack " + msgId);
		}

		/**
		 * 当后继者bolt发送fail时，这里的fail方法会被调用
		 * 一般来说fail之后会重新发射数据到bolt
		 */
		@Override
		public void fail(Object msgId) {
			super.fail(msgId);
			System.out.println("调用了fail " + msgId);
		}
	}

	public static class SumBolt extends BaseRichBolt {
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;

		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}

		/**
		 * 死循环，用于接收bolt送来的数据
		 */
		int sum = 0;

		public void execute(Tuple input) {
			Integer value = input.getIntegerByField("v1");
			sum += value;
			System.err.println(Thread.currentThread().getId() + "\t" + value);

			if (value > 10 && value < 20) {
				this.collector.fail(input);
			} else {
				this.collector.ack(input);
			}

			//实际开发中ack和fail方法的调用应该放在try-catch里
			// try {
			// 		value = tuple.getIntegerByField("v1");
			// 		sum += value;
			// 		System.err.println(Thread.currentThread().getId() + "\t" + value);
			//
			// 		this.collector.ack(tuple);
			// } catch (Exception e) {
			// 		this.collector.fail(tuple);
			// }
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {

		}
	}
}
