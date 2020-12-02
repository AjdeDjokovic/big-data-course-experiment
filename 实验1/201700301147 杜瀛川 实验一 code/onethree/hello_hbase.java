package hbase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

public class hello_hbase {
	public static Configuration conf = null;
	  public HTable table = null;
	  public HBaseAdmin admin = null;

	  static {
	    conf = HBaseConfiguration.create();
	    System.out.println(conf.get("hbase.zookeeper.quorum"));
	  }
	  /**
	   * 创建一张表
	   */
	  public static void createTable(String tableName, String[] familys)
	      throws Exception {
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    if (admin.tableExists(tableName)) {
	      System.out.println("table already exists!");
	      //如果表格存在先删除再新建表格
	      //deleteTable(tableName);
	      createTable(tableName,familys);
	    } else {
	      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
	      for (int i = 0; i < familys.length; i++) {
	        tableDesc.addFamily(new HColumnDescriptor(familys[i]));
	      }
	      admin.createTable(tableDesc);
	      System.out.println("create table " + tableName + " ok.");
	    }
	  }
	  public static void main(String[] args) {
		    // TODO Auto-generated method stub
		     try {
		          String tablename = "abc";
		          String[] familys = { "a", "b" };
		          hello_hbase.createTable(tablename, familys);

		         
		        } catch (Exception e) {
		          e.printStackTrace();
		        }
		      }


}
