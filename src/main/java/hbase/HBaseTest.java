package hbase;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseTest {

	private static final String TABLE_NAME = "t3";
	private static final String FAMILY_NAME = "f1";
	private static final String ROW_KEY1 = "r1";
	private static final String ROW_KEY2 = "r2";
	private static final String COLUMN_NAME = "name";
	private static final String COLUMN_AGE = "age";

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://hadoop0:9000/hbase");
		conf.set("hbase.zookeeper.quorum", "hadoop0");
		
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		//create a hbase table
		if (!hBaseAdmin.tableExists(TABLE_NAME)) {
			HTableDescriptor desc = new HTableDescriptor(TABLE_NAME.getBytes());
			HColumnDescriptor family = new HColumnDescriptor(FAMILY_NAME.getBytes());
			desc.addFamily(family);
			hBaseAdmin.createTable(desc);
		}
		
		//increase records to table
		HTable hTable = new HTable(conf, TABLE_NAME.getBytes());
		/*Put put1 = new Put(ROW_KEY1.getBytes());
		put1.add(FAMILY_NAME.getBytes(), COLUMN_NAME.getBytes(), "zhangsan".getBytes());
		put1.add(FAMILY_NAME.getBytes(), COLUMN_AGE.getBytes(), "23".getBytes());
		
		Put put2 = new Put(ROW_KEY2.getBytes());
		put2.add(FAMILY_NAME.getBytes(), COLUMN_NAME.getBytes(), "lisi".getBytes());
		put2.add(FAMILY_NAME.getBytes(), COLUMN_AGE.getBytes(), "24".getBytes());
		
		ArrayList<Put> putList = new ArrayList<Put>();
		putList.add(put1);
		putList.add(put2);
		
//		hTable.put(put1);
//		hTable.put(put2);
		hTable.put(putList);*/
		
		//query one record
		/*Get get = new Get(ROW_KEY1.getBytes());
		Result result = hTable.get(get);
		String name = new String(result.getValue(FAMILY_NAME.getBytes(), COLUMN_NAME.getBytes()));
		String age = new String(result.getValue(FAMILY_NAME.getBytes(), COLUMN_AGE.getBytes()));
		System.out.println(result+"\t"+name+"\t"+age);*/
		
		//query all the records of the table  
		Scan scan = new Scan();
		//过滤列
		scan.addColumn(FAMILY_NAME.getBytes(), COLUMN_NAME.getBytes());
		//过滤行
		scan.setStartRow(ROW_KEY1.getBytes());
//		scan.setStopRow(ROW_KEY2.getBytes());
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			String rowKey = new String(result.getRow());
			String name = new String(result.getValue(FAMILY_NAME.getBytes(), COLUMN_NAME.getBytes()));
//			String age = new String(result.getValue(FAMILY_NAME.getBytes(), COLUMN_AGE.getBytes()));
			System.out.println(result+"\t"+rowKey+"\t"+name/*+"\t"+age*/);
		}
		
		//delete one record
		Delete delete = new Delete(ROW_KEY1.getBytes());
		hTable.delete(delete);
		
//		hBaseAdmin.disableTable(TABLE_NAME);
//		hBaseAdmin.deleteTable(TABLE_NAME);
		
		hTable.close();
		hBaseAdmin.close();
	}

}
