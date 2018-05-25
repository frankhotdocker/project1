package ratpack.example.java;

/**
 * The service implementation.
 *
 * @see MyHandler
 */
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class MyServiceImpl implements MyService {

    public String getValue() {
        SparkConf conf = new SparkConf(true)
                //.set("spark.local.ip","172.20.0.1") // helps when multiple network interfaces are present. The driver must be in the same network as the master and slaves
                //.set("spark.driver.host","172.20.0.1") // same as above. This duality might disappear in a future version
                .set("spark.cassandra.connection.host", "172.29.0.228")
                .set("spark.cassandra.connection.port", "9042")
                .set("deploy-mode" ,"client")
                .set("spark.executor.memory", "6G")
                .set("spark.driver.memory", "14G")
                .set("spark.driver.maxResultSize", "10G")
        ;

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark SQL examples")
                .master("local")
                .config(conf)
                .getOrCreate();

//        Dataset<Row> data = spark.read().json("/home/frank/data.json");
        Map table = new HashMap();
        table.put( "table","decision_history_by_msisdn");
        table.put("keyspace", "trex");

        Dataset<Row> data = spark.read().format("org.apache.spark.sql.cassandra").options(table).load();
        data.show();
        data.printSchema();


        Dataset<Row> decHist = spark
                .read()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of( "table", "decision_history_by_msisdn", "keyspace", "trex"))
                .load();

        decHist.createOrReplaceTempView( "decHist");

        Dataset<Row> count = spark.sql("Select count(*) from decHist");

        String result = "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>" +
                "<title>Title</title></head><body>"+count.first().toString()+ "</body></html>";

        spark.stop();
        return result;
    }

}
