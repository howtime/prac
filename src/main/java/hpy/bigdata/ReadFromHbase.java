package hpy.bigdata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.com.esotericsoftware.minlog.Log;

public class ReadFromHbase {
    private static Configuration configuration;
    private static User user;
    private static Connection conn;
    private static Admin admin;
    private static final String hbaseusername = "yarn";
    ReadFromHbase()
    {
        configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","test1,test2,test3");
        try{
            user = User.create(UserGroupInformation.createRemoteUser(hbaseusername));
            conn = ConnectionFactory.createConnection(configuration,user);
            admin = conn.getAdmin();
        }catch(IOException e){
            //TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
//    static{
//        configuration=HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum","test1,test2,test3");
//        try{
//            user = User.create(UserGroupInformation.createRemoteUser(hbaseusername));
//            conn = ConnectionFactory.createConnection(configuration,user);
//            admin = conn.getAdmin();
//        }catch(IOException e){
//            //TODO Auto-generated catch block
//            e.printStackTrace();
//
//    }
    public void ReadDataFromHbaseTable(String tableName,String rowkey) throws IOException
    {
        HTable table = new HTable(configuration,"testtb");
        Get get = new Get(Bytes.toBytes((rowkey)));
        get.addFamily(Bytes.toBytes("id"));
        Result result = table.get(get);
        byte[] val = result.getValue(Bytes.toBytes("id"),Bytes.toBytes("name"));
        System.out.println("Value: " + Bytes.toString(val));

    }

    public void scanTable(String TableName)
    {
        try
        {
            HTable table = new HTable(configuration,TableName);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("1"));
            scan.setStopRow(Bytes.toBytes("6"));
            ResultScanner rs = table.getScanner(scan);
            for(Result result : rs)
            {
                for(Cell cell:result.rawCells())
                {
                    System.out.println(new String(CellUtil.cloneRow(cell)) + "\t"
                            + new String(CellUtil.cloneFamily(cell))+"\t"
                            + new String(CellUtil.cloneQualifier(cell))+"\t"
                            + new String(CellUtil.cloneValue(cell),"UTF-8")+"\t"
                            + cell.getTimestamp()
                    );
                }
            }
            table.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

}


