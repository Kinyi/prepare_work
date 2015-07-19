package storm;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

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
import backtype.storm.utils.Utils;

/**
 * 作业：实现单词计数。 (1)要求从一个文件夹中把所有文件都读取，计算所有文件中的单词出现次数。
 * (2)当文件夹中的文件数量增加是，实时计算所有文件中的单词出现次数。
 */
public class WordCount {

	public static class DataSourceSpout extends BaseRichSpout {
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;

		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}

		public void nextTuple() {
			Collection<File> listFiles = FileUtils.listFiles(new File("E://zdir"), new String[] { "txt" }, true);
			for (File file : listFiles) {
				String fileContent = "";
				try {
					fileContent = FileUtils.readFileToString(file);
				} catch (IOException e) {
					e.printStackTrace();
				}
				String[] words = fileContent.split("\\s");
				for (String word : words) {
					if (!"".equals(word)) {
						collector.emit(new Values(word));
						Utils.sleep(1000);
					}
				}
				try {
					FileUtils.moveFile(file, new File(file.getAbsolutePath()+"."+System.currentTimeMillis()));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("v1"));
		}

	}

	public static class WordCountBolt extends BaseRichBolt {
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;

		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}

		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

		public void execute(Tuple input) {
			String value = input.getStringByField("v1");
			Integer time = hashMap.get(value);
			if (time == null) {
				hashMap.put(value, 1);
			} else {
				time++;
				hashMap.put(value, time);
			}
			Iterator<String> iterator = hashMap.keySet().iterator();
			System.out.println("=====================================");
			while (iterator.hasNext()) {
				String key = iterator.next();
				Integer times = hashMap.get(key);
				System.out.println(key + ":" + times);
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {

		}
	}

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("1", new DataSourceSpout());
		builder.setBolt("2", new WordCountBolt()).shuffleGrouping("1");

		LocalCluster localCluster = new LocalCluster();
		Config config = new Config();
		localCluster.submitTopology(WordCount.class.getSimpleName(), config,
				builder.createTopology());

		try {
			TimeUnit.SECONDS.sleep(3600);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		localCluster.shutdown();
	}
}
