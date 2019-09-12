package hpy.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class sparkOnHbase {
    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_USER","root");
        System.setProperty("spark.serializer","org.apache.spark.serializer.kryoSerializer");
        SparkSession sparkSession = SparkSession.builder()
                .appName("hpy_java_sparksql_on_hbase")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());

        Configuration configuration = HBaseConfiguration.create();


    }

}
