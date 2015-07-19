package storm;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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

public class LocalStormTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("input", new DataSourceSpout());
		builder.setBolt("sum", new SumBolt()).shuffleGrouping("input");
		
		LocalCluster localCluster = new LocalCluster();
		Config config = new Config();
		localCluster.submitTopology(LocalStormTopology.class.getSimpleName(), config, builder.createTopology());
		
		try {
			TimeUnit.SECONDS.sleep(3600);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		localCluster.shutdown();
	}
	
	public static class DataSourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;

		/**
		 * 在本实例运行时，首先被调用
		 */
		public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}

		/**
		 * 认为是heartbeat，永无休息，死循环的调用。线程安全的操作。
		 */
		int i = 0;
		public void nextTuple() {
			//送出去，送给bolt
			//Values是一个value的List
			System.err.println("spout:"+i);
			collector.emit(new Values(i++));
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//Fields是一个field的List
			declarer.declare(new Fields("v1"));
		}
	}
	
	public static class SumBolt extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;

		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
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
			System.err.println("sum:"+sum);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
	}
}
