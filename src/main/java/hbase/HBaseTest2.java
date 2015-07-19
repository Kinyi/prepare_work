package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;

public class HBaseTest2 {

	private static final String TABLE_NAME = "wlan";

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://hadoop0:9000/hbase");
		conf.set("hbase.zookeeper.quorum", "hadoop0");
		
		HTable hTable = new HTable(conf, TABLE_NAME);
		
		Scan scan = new Scan();
		//使用filter过滤数据
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^\\d*[:]201303122359\\d*$"));
		scan.setFilter(rowFilter);
		
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			String rowKey = new String(result.getRow());
			
			System.out.println(rowKey);
		}
		
		
		hTable.close();
	}

}
