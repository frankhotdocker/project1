package ratpack.example.java;

import com.datastax.driver.core.*;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Promise;
import ratpack.handling.Context;
import ratpack.handling.Handler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;


public class MyHandler implements Handler {
    static Logger logger = LoggerFactory.getLogger(MyHandler.class.getName());

    public static String escapeQueryChars(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            // These characters are part of the query syntax and must be escaped
            if (c == '\\' || c == '+' || c == '-' || c == '!'  || c == '(' || c == ')' || c == ':'
                    || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
                    || c == '*' || c == '?' || c == '|' || c == '&'  || c == ';' || c == '/'
                    || Character.isWhitespace(c)) {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    @Override
    public void handle(Context context) {
/*
    SparkConf conf=context.get(SparkConf.class);

    SparkSession spark = SparkSession
            .builder()
            .appName("Spark SQL examples")
            .master("local[4]")
            .config(conf)
            .getOrCreate();
*/
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
        Cluster cluster = context.get(Cluster.class);
        Session sess = cluster.connect("artemis");
//    CassandraConnector connector =  CassandraConnector.apply(conf);
//    Session sess = connector.openSession();
        try {
            sess.execute("CREATE KEYSPACE IF NOT EXISTS test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
            sess.execute("CREATE TABLE IF NOT EXISTS test2.words (word text PRIMARY KEY, count int)");
        } catch (Exception e) {
        }
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
/*
    Dataset<Row> decHist = spark
            .read()
            .format("org.apache.spark.sql.cassandra")
//            .options(ImmutableMap.of( "table", "decision_history_by_msisdn", "keyspace", "trex"))
            .options(ImmutableMap.of( "table", "event", "keyspace", "artemis")) //, "user" , "trex", "password", "trex" ))
            .load();

    decHist.createOrReplaceTempView( "decHist");
    String sql="Select * from decHist where solr_query='dfptoken:d*' limit 2";
    Dataset<Row> count = spark.sql(pSql==null||pSql.isEmpty()?sql:pSql);
*/


        String statement = "Select * from artemis.event where solr_query=? limit 5";
        String solr = "";

        String dfpExactId = "XX_AegiSAsLi9eQCvg6zE22dRc73lUc";
        String dfpSmartId = "AP_S5KwmarkZ_duE1SQJX1lhVoLI";
        String caNumber = "x";
        String caIban = "x";
        String caEmail = "x";
        String address_type = "*";
        String street = "'Am Probsthof'";
        String street2 = "*";

        solr = "{"
                + "'q':"
                + " '"
                + "{!tuple "
                + " v=\""
                + "order_addresses.address_type:" + address_type
                + " AND order_addresses.street:" + street2
                + "\"}"
//                + " OR "
//                + "dfp_smart_id:" + dfpSmartId
//                + " OR "
//                + "dfp_exact_id:" + dfpExactId
                + " '" //end q
                + ",'sort':'total_score desc'"
                + ", 'facet':{'field':['event_status', 'total_score', 'event_type', 'dfp_exact_id', 'dfp_smart_id', 'rmc_person_number', 'customer_account_number', 'customer_account_iban', 'customer_account_email']}"
                +" ,'timeAllowed':3000"
//                + ",'start':'1'"
                + "}";

        //solr=escapeQueryChars(solr);



        solr="{'q':'dfp_token:*', 'sort':'total_score desc'}";
        logger.debug("solr:" + solr);
        PreparedStatement x = sess.prepare(statement);
        BoundStatement bound = x.bind(solr);

        Promise<ResultSet> testCassandra = Promise.async(upstream -> {
            ResultSetFuture resultSetFuture = sess.executeAsync(bound);
            upstream.accept(resultSetFuture);
        });

//    Promise<ResultSet> testCassandra = Promise.async(upstream -> {
//      ResultSetFuture resultSetFuture = sess.executeAsync(pSql==null||pSql.isEmpty()?statement:pSql);
//      upstream.accept(resultSetFuture);
//    });

/*
        String urlString = "http://seed-solr-fheinen.cloud.scoop-gmbh.de/solr/artemis.event";
        HttpSolrClient solrCli = new HttpSolrClient.Builder(urlString).build();
        solrCli.setParser(new XMLResponseParser());

        String username = "cassandra";
        String password = "cassandra";

        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");

        SolrRequest<QueryResponse> req = new QueryRequest(query);
        req.setBasicAuthCredentials(username, password);

        QueryResponse response = null;
        try {
            response = req.process(solrCli);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        SolrDocumentList docList = response.getResults();
        logger.debug("Found {}", docList.getNumFound());

        for (SolrDocument doc : docList) {
            logger.debug(doc.toString());
        }
*/
        String[] xx = {"Start: " + ((pSql == null || pSql.isEmpty()) ? statement : pSql) + " solr_query" + solr + "\n"};

        testCassandra.map(resultSet -> {
            resultSet.all().forEach(row -> xx[0] = xx[0] + "\n" + row.toString());
            return xx[0];
        })
                .then(context::render);

/*
    String[] xxx= {"Start: " + (pSql==null||pSql.isEmpty()?sql:pSql) + "\n"};
    Blocking.get(count::collectAsList)
            .map(ls -> {ls.forEach(row -> xxx[0]=xxx[0]+row.toString()+"\n");return xxx[0]+"End\n";})
            .then(x -> context.render(x+xx[0]));
*/
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
