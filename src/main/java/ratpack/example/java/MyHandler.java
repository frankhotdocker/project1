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
            .master("local[4]")
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
    String pSql = context.getAllPathTokens().get("sql");

    Dataset<Row> decHist = spark
            .read()
            .format("org.apache.spark.sql.cassandra")
            .options(ImmutableMap.of( "table", "decision_history_by_msisdn", "keyspace", "trex"))
            .load();

    decHist.createOrReplaceTempView( "decHist");
    String sql="Select * from decHist where bp_number='100' order by decision_ts desc limit 500";
    Dataset<Row> count = spark.sql(pSql==null||pSql.isEmpty()?sql:pSql);

    String[] xxx= {"Start: " + (pSql==null||pSql.isEmpty()?sql:pSql) + "\n"};
    Blocking.get(() -> count.collectAsList())
            .map(ls -> {ls.forEach(row -> xxx[0]=xxx[0]+row.toString()+"\n");return xxx[0]+"End\n";})
            .then(x->context.render(x));

/*
    String statement="Select msisdn from decision_history_by_msisdn order by decision_ts desc limit 2";
    Promise<ResultSet> testCassandra = Promise.async(upstream -> {
      ResultSetFuture resultSetFuture = session.executeAsync(statement);
      upstream.accept(resultSetFuture);
    });

    testCassandra.map(resultSet -> {resultSet.all().forEach(row -> xxx[0]=xxx[0]+"\n"+row.getString("msisdn"));return xxx[0]; })
            .then(x->context.render(x));
*/
  }
}
