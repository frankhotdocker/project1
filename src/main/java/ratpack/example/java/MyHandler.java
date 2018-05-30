package ratpack.example.java;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.handling.Context;
import ratpack.handling.Handler;

import java.util.HashMap;
import java.util.Map;


public class MyHandler implements Handler {


  @Override
  public void handle(Context context) {
    SparkConf conf=context.get(SparkConf.class);

    SparkSession spark = SparkSession
            .builder()
            .appName("Spark SQL examples")
            .master("local")
            .config(conf)
            .getOrCreate();


    Cluster cluster =context.get(Cluster.class);
    Session session = cluster.connect("trex");

    /*
    Map table = new HashMap();
    table.put( "table","decision_history_by_msisdn");
    table.put("keyspace", "trex");

    Dataset<Row> data = spark.read().format("org.apache.spark.sql.cassandra").options(table).load();
    data.show();
    data.printSchema();
*/

    Dataset<Row> decHist = spark
            .read()
            .format("org.apache.spark.sql.cassandra")
            .options(ImmutableMap.of( "table", "decision_history_by_msisdn", "keyspace", "trex"))
            .load();

    decHist.createOrReplaceTempView( "decHist");
    String sql="Select count(*) from decHist";
    Dataset<Row> count = spark.sql(sql);

//    Promise<String> decCount = Promise.async(down ->
//                    new Thread(() -> {
//                      down.success(sql + " : " + count.first().toString());
//                    }).start());

    String[] xxx= {""};
    Blocking.get(() -> sql + " : " + count.first().toString()).then(x -> xxx[0]=x);

    String statement="Select msisdn from decision_history_by_msisdn limit 1";
    Promise<ResultSet> testCassandra = Promise.async(upstream -> {
      ResultSetFuture resultSetFuture = session.executeAsync(statement);
      upstream.accept(resultSetFuture);
    });

    testCassandra.map(resultSet -> xxx[0]+ " -:- Select msisdn from decision_history_by_msisdn limit 1 " + resultSet.one().getString("msisdn")).then(x->context.render(x));
  }
}
