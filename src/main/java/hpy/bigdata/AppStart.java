package hpy.bigdata;


public class AppStart {
    public static void main(String args[])throws Exception
    {
//        ReadFromHbase readdata = new ReadFromHbase();
//        readdata.scanTable("testtb");
        OperateHive oprthive = new OperateHive();
        oprthive.init();
        //oprthive.tablearch("test_hive");
        oprthive.queryTableData("test_hive");
    }
}
