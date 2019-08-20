package ratpack.example.java;

import com.datastax.driver.core.Cluster;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.error.ServerErrorHandler;
import ratpack.guice.Guice;
import ratpack.handling.Chain;
import ratpack.handling.Context;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;

import java.util.HashMap;
import java.util.Map;


public class MyApp {
    static Logger logger = LoggerFactory.getLogger(MyApp.class.getName());

    public static void main(String[] args) throws Exception {

        logger.info("XX\nXXXXX");
        String test= System.getenv("CASSANDRA_SEED");
        final String seed= (test!=null && !test.isEmpty())?test:"localhost";
        Cluster.Builder builder = Cluster.builder();
        builder.withCredentials("cassandra".trim(), "cassandra".trim());
        builder.addContactPoint(seed);
        Cluster cluster= builder.build();

        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", seed)
                .set("spark.cassandra.connection.port", "9042")
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra")
                .set("deploy-mode" ,"client")
                .set("spark.executor.memory", "6G")
                .set("spark.driver.memory", "14G")
                .set("spark.driver.maxResultSize", "10G")
                .set("spark.dse.continuous_paging_enabled","true")
                .set("spark.cassandra.read.timeout_ms", "60000")
                ;

        RatpackServer.start(s -> s
            .serverConfig(c -> c.baseDir(BaseDir.find()))
            .registry(Guice.registry(r -> {
                r.bindInstance(SparkConf.class, conf);
                r.bindInstance(Cluster.class, cluster);
                r.bind(ServerErrorHandler.class, ErrorHandler.class);
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
                .path("cassandra/:sql", MyHandler.class) // Map to a dependency injected handler
                .prefix("static",
                        path -> path.fileSystem("assets/images", Chain::files)) // Bind the /static app path to the src/ratpack/assets/images dir
                .all(ctx -> ctx.render("root handler!"))
            )
        );
    }

    static public class ErrorHandler implements ServerErrorHandler {

        @Override public void error(Context context, Throwable throwable) throws Exception {
            try {
                Map<String, String> errors = new HashMap<>();

                errors.put("error", throwable.getClass().getCanonicalName());
                errors.put("message", throwable.getMessage());

                Gson gson = new GsonBuilder().serializeNulls().create();

                context.getResponse().status(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).send(gson.toJson(errors));
                throw throwable;
            } catch (Throwable throwable1) {
                throwable1.printStackTrace();
            }
        }

    }
}
