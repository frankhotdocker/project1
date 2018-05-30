package ratpack.example.java;

import com.datastax.driver.core.Cluster;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import ratpack.guice.Guice;
import ratpack.handling.Chain;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;

import java.util.Map;


public class MyApp {

    public static void main(String[] args) throws Exception {
        String test= System.getenv("CASSANDRA_SEED");
        final String seed= (test!=null && !test.isEmpty())?test:"localhost";
        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoint(seed);
        Cluster cluster= builder.build();

        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", seed)
                .set("spark.cassandra.connection.port", "9042")
                .set("deploy-mode" ,"client")
                .set("spark.executor.memory", "6G")
                .set("spark.driver.memory", "14G")
                .set("spark.driver.maxResultSize", "10G")
                ;

        RatpackServer.start(s -> s
            .serverConfig(c -> c.baseDir(BaseDir.find()))
            .registry(Guice.registry(r -> {
                r.bindInstance(SparkConf.class, conf);
                r.bindInstance(Cluster.class, cluster);
                r.bind(MyHandler.class);
            }))
            .handlers(chain -> chain
                .path("bar", ctx -> ctx.render("from the bar handler")) // Map to /bar
                .prefix("nested", nested -> { // Set up a nested routing block, which is delegated to `nestedHandler`
                    nested.path(":var1/:var2?", ctx -> { // The path tokens are the :var1 and :var2 path components above
                        Map<String, String> pathTokens = ctx.getPathTokens();
                        ctx.render(
                        "from the nested handler, var1: " + pathTokens.get("var1") +
                                ", var2: " + pathTokens.get("var2")
                        );
                    });
                })
                .path("cassandra", MyHandler.class) // Map to a dependency injected handler
                .prefix("static",
                        nested -> nested.fileSystem("assets/images", Chain::files)) // Bind the /static app path to the src/ratpack/assets/images dir
                .all(ctx -> ctx.render("root handler!"))
            )
        );
    }
}
