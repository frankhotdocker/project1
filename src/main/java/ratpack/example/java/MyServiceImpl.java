package ratpack.example.java;

/**
 * The service implementation.
 *
 * @see MyHandler
 */
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.stringtemplate.v4.ST;
import ratpack.exec.Promise;
import ratpack.handling.Context;

import java.util.HashMap;
import java.util.Map;

public class MyServiceImpl implements MyService {

    public Promise<String> getValue(Context context) {
        SparkSession spark=context.get(SparkSession.class);
        Session session =context.get(Session.class);

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
        String sql="Select count(*) from decHist";
        Dataset<Row> count = spark.sql(sql);

        String statement="Select msisdn from decision_history_by_msisdn limit 2";

        Promise<ResultSet> testCassandra = Promise.async(upstream -> {
            ResultSetFuture resultSetFuture = session.executeAsync(statement);
            upstream.accept(resultSetFuture);
        });

        String result = sql + " : " + count.first().toString();

        return testCassandra.map(resultSet -> result+ "-:-" + resultSet.one().getString("msisdn"));

    }

}
