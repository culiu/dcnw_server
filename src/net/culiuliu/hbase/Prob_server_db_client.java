package net.culiuliu.hbase;

import java.io.IOException;

import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Prob_server_db_client 
{
  public static void insert_stats(String json_str)
  {
	  System.out.println(json_str);
	  
	  Configuration config = HBaseConfiguration.create();
      config.addResource(new Path("hbase-site.xml"));
	  
	  String tableName = "network_profiling";
	  
	  try {
		Connection conn = ConnectionFactory.createConnection(config);
		Admin admin = conn.getAdmin();
		System.out.println("got Admin!");
		if (! admin.tableExists(TableName.valueOf(tableName))) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			tableDescriptor.addFamily(new HColumnDescriptor("ping_stats"));
			admin.createTable(tableDescriptor);
			System.out.println("Table created");
		}

		Table table = conn.getTable(TableName.valueOf(tableName));
		
		String stats = "record-";
		
		index++;
		
		Put p = new Put(Bytes.toBytes(stats+index));
		
		JSONParser parser = new JSONParser();
		JSONObject jsonObj = (JSONObject) parser.parse(json_str);
		JSONArray ping_stat_array = (JSONArray) jsonObj.get("ping_stats");

		for (Object sing_ping_stat : ping_stat_array) {
			String src_ip = (String) (((JSONObject) sing_ping_stat).get("src_ip"));
			String dst_ip = (String) (((JSONObject) sing_ping_stat).get("dst_ip"));
			String src_port = (String) (((JSONObject) sing_ping_stat).get("src_port"));
			String dst_port = (String) (((JSONObject) sing_ping_stat).get("dst_port"));
			String timestamp = (String) (((JSONObject) sing_ping_stat).get("time"));
			String rtt = (String) (((JSONObject) sing_ping_stat).get("RTT"));
			
			
			p.add(Bytes.toBytes("ping_stats"), Bytes.toBytes("src_ip"), Bytes.toBytes(src_ip));
			p.add(Bytes.toBytes("ping_stats"), Bytes.toBytes("dst_ip"), Bytes.toBytes(dst_ip));
			p.add(Bytes.toBytes("ping_stats"), Bytes.toBytes("src_port"), Bytes.toBytes(src_port));
			p.add(Bytes.toBytes("ping_stats"), Bytes.toBytes("dst_port"), Bytes.toBytes(dst_port));
			p.add(Bytes.toBytes("ping_stats"), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
			p.add(Bytes.toBytes("ping_stats"), Bytes.toBytes("rtt"), Bytes.toBytes(rtt));
		}
		
		table.put(p);
		System.out.println("record inserted");
		table.close();
		conn.close();
		System.out.println("done");
	  } catch (IOException e) {
			// TODO Auto-generated catch block
		  System.out.println(tableName + " does not exists.");
			e.printStackTrace();
	  } catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
  }
  
  private static long index = 0;
}
