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
import org.stringtemplate.v4.ST;
import ratpack.exec.Promise;
import ratpack.handling.Context;

import java.util.HashMap;
import java.util.Map;

public class MyServiceImpl implements MyService {

    public String getValue(Context context) {
        SparkSession spark=context.get(SparkSession.class);
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

        String result = sql + " : " + count.first().toString();

        return result;
    }

}
