package ratpack.example.java;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.handling.Context;
import ratpack.handling.Handler;


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

/*
    Dataset<Row> empDF = spark
            .read()
            .format("jdbc")
            .option("url", "jdbc:oracle:thin:@localhost:1521:rmcdev")
            .option("dbtable", "RSK_SCHEMA.TA_DECISION_HISTORY")
            .option("user", "RSK_SCHEMA")
            .option("password", "RSK_SCHEMA")
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .load();
*/
    Cluster cluster =context.get(Cluster.class);
    Session session = cluster.connect("wiki");
    CassandraConnector connector =  CassandraConnector.apply(spark.sparkContext().conf());
    Session sess = connector.openSession();
    try {
      sess.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
      sess.execute("CREATE TABLE test2.words (word text PRIMARY KEY, count int)");
    }catch (Exception e){}
/*
    Map table = new HashMap();
    table.put( "table","decision_history_by_msisdn");
    table.put("keyspace", "trex");

    Dataset<Row> data = spark.read().format("org.apache.spark.sql.cassandra").options(table).load();
    data.show();
    data.printSchema();
*/
/**/
    String pSql = context.getAllPathTokens().get("sql");

    Dataset<Row> decHist = spark
            .read()
            .format("org.apache.spark.sql.cassandra")
//            .options(ImmutableMap.of( "table", "decision_history_by_msisdn", "keyspace", "trex"))
            .options(ImmutableMap.of( "table", "solr", "keyspace", "wiki", "user" , "trex", "password", "trex" ))
            .load();

    decHist.createOrReplaceTempView( "decHist");
    String sql="Select * from decHist where solr_query='title:Afric?' limit 50";
    Dataset<Row> count = spark.sql(pSql==null||pSql.isEmpty()?sql:pSql);


    String[] xxx= {"Start: " + (pSql==null||pSql.isEmpty()?sql:pSql) + "\n"};
    Blocking.get(count::collectAsList)
            .map(ls -> {ls.forEach(row -> xxx[0]=xxx[0]+row.toString()+"\n");return xxx[0]+"End\n";})
            .then(context::render);
/**/
/*
    String statement="Select msisdn from trex.decision_history_by_msisdn limit 2";
    Promise<ResultSet> testCassandra = Promise.async(upstream -> {
      ResultSetFuture resultSetFuture = session.executeAsync(statement);
      upstream.accept(resultSetFuture);
    });
    String[] xxx= {"Start: " + "\n"};

    testCassandra.map(resultSet -> {resultSet.all().forEach(row -> xxx[0]=xxx[0]+"\n"+row.getString("msisdn"));return xxx[0]; })
            .then(x->context.render(x));
*/
  }
}
