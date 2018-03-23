package hpy.bigdata;

import org.apache.tools.ant.taskdefs.Echo;

import javax.xml.transform.Result;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class OperateHive {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://10.12.18.181:10000/default";
    private static String user = "yarn";
    private static String password = "";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public void init() throws Exception{

        Class.forName(driverName);
        conn=DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();
    }

    public void createDatabase() throws Exception{
        String sql = "Create database Hpy_HiveTest_Db";
        System.out.println("Running: " + sql);
        stmt.execute(sql);

    }
    public void showDatabases() throws Exception{
        String sql = "show databases;";
        System.out.println();
        rs = stmt.executeQuery(sql);
        while(rs.next())
        {
            System.out.println(rs.getString(1));
        }
    }

    public void createTable() throws Exception{
        String sql = "create table hpy_HiveTest_tb(\n "+
                "empno int,\n"+
                "ename string,\n"+
                "job string,\n"+
                "mgr int,\n"+
                "hiredate string,\n"+
                "sal double,\n" +
                "comm double,\n"+
                "deptno int\n"+
                ")\n"+
                "row format delimited fields terminated by '\\t'";
        System.out.println("Running: "+ sql);
        stmt.execute(sql);

    }
    public void showTables() throws Exception{
        String sql = "show tables";
        System.out.println("Running: "+sql);
        rs=stmt.executeQuery(sql);
        while(rs.next())
        {
            System.out.println(rs.getString(1));
        }
    }
    public void queryTableData(String tablename) throws Exception{
        String sql = "select * from "+tablename;
        System.out.println("running: " + sql);
        rs= stmt.executeQuery(sql);
        System.out.println("车号"+"\t"+"站号"+"\t"+"时间");
        while(rs.next())
        {
            System.out.println(rs.getString("trainid")+"\t"+rs.getString("coachid")
                    +"\t"+rs.getString("time"));
        }
    }
    public void tablearch(String tablename) throws Exception{
        String sql = "desc "+tablename;
        System.out.println("Running: "+sql);
        rs=stmt.executeQuery(sql);

        while(rs.next())
        {
            System.out.println(rs.getString(1));
        }
    }
}
