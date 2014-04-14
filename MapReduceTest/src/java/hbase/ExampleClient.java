package hbase;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {

	static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml");
	}

	public static void main(String[] args) throws Exception {
		//printConfig();
		//getTableInfo();
		//getOneDataByRowKey("GFMetaS","0000002827");
		
		getAllInfo("GFMetaS");
	}
	/*
	 * 根据键值查找对应的记录
	 */
	
	public static void getOneDataByRowKey(String tableName,String rowkey) throws Exception  {
		HTable h = new HTable(conf, tableName);
		Get g = new Get(Bytes.toBytes(rowkey));
		Result r = h.get(g);
		for(KeyValue kv :r.raw()){
			System.out.println(Bytes.toStringBinary(kv.getRow()));
			System.out.println("时间戳:  " + kv.getTimestamp());
			System.out.println("列簇:  " + Bytes.toStringBinary(kv.getFamily()));
			System.out.println("列:   " + Bytes.toStringBinary(kv.getQualifier()));
			if(Bytes.toStringBinary(kv.getQualifier()).equals("resolution") || Bytes.toStringBinary(kv.getQualifier()).equals("datasize")){
				System.out.println("值：   " +  Bytes.toInt(kv.getValue()));
				
			}
			else if(Bytes.toStringBinary(kv.getQualifier()).equals("cloudcover")){
				System.out.println("值：   " +  Bytes.toDouble(kv.getValue()));
			}
			else if(Bytes.toStringBinary(kv.getQualifier()).equals("cloudcover")){
				System.out.println("值：   " +  Bytes.toDouble(kv.getValue()));
			}
			else if(Bytes.toStringBinary(kv.getQualifier()).equals("ctime")){
				System.out.println("值：   " +  new Date(Bytes.toLong(kv.getValue())).toLocaleString());
			}
			else
			{
				System.out.println("值：   " +  Bytes.toStringBinary(kv.getValue()));
			}
			System.out.println("===============================================");
		}
		h.close();
	}
	
	
	/*
	 * 获取所有的信息
	 */
	public static void getAllInfo(String tableName) throws IOException{
		HTable h = null;
			h = new HTable(conf,tableName);
			//Scan scan = new Scan();
			Scan scan=new Scan(Bytes.toBytes("0000002827"),Bytes.toBytes("0000002828"));
			ResultScanner scanner = h.getScanner(scan);
			
			for(Result r : scanner){
				System.out.println("#########################################################################");
				for(KeyValue kv :r.raw()){
					System.out.println(Bytes.toStringBinary(kv.getRow()));
					System.out.println("时间戳:  " + kv.getTimestamp());
					System.out.println("列簇:  " + Bytes.toStringBinary(kv.getFamily()));
					System.out.println("列:   " + Bytes.toStringBinary(kv.getQualifier()));
					if(Bytes.toStringBinary(kv.getQualifier()).equals("resolution") || Bytes.toStringBinary(kv.getQualifier()).equals("datasize")){
						System.out.println("值：   " +  Bytes.toInt(kv.getValue()));
						
					}
					else if(Bytes.toStringBinary(kv.getQualifier()).equals("cloudcover")){
						System.out.println("值：   " +  Bytes.toDouble(kv.getValue()));
					}
					else if(Bytes.toStringBinary(kv.getQualifier()).equals("cloudcover")){
						System.out.println("值：   " +  Bytes.toDouble(kv.getValue()));
					}
					else if(Bytes.toStringBinary(kv.getQualifier()).equals("ctime")){
						System.out.println("值：   " +  new Date(Bytes.toLong(kv.getValue())).toLocaleString());
					}
					else
					{
						System.out.println("值：   " +  Bytes.toStringBinary(kv.getValue()));
					}
					System.out.println("===============================================");
				}
			}
			h.close();
	}
	/*
	 * 获取表结构信息
	 */
	private static void getTableInfo() {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes
					.toBytes("GFMetaS"));
			byte[] name = tableDescriptor.getName();
			System.out.println("result");
			System.out.println("table name:" + new String(name));
			System.out.println("FamiliesKeys:");
			Set<byte[]> fuck = tableDescriptor.getFamiliesKeys();
			Iterator<byte[]> it = fuck.iterator();
			while (it.hasNext()) {
				System.out.println(new String(it.next()));
			}
			
			HColumnDescriptor[] colimnFamilies = tableDescriptor.getColumnFamilies();
			for(HColumnDescriptor hd:colimnFamilies){
				System.out.println("colimn families:"+hd.getNameAsString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * 获取配置信息
	 */
	public static void printConfig() {
		for (Iterator<Entry<String, String>> it = conf.iterator(); it
				.hasNext();) {
			Entry<String, String> kv = it.next();
			System.out.println(kv.getKey() + " " + kv.getValue());
		}
	}

}
