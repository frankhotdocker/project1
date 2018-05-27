package ratpack.example.java;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import ratpack.exec.ExecResult;
import ratpack.exec.Promise;
import ratpack.exec.util.Promised;
import ratpack.guice.Guice;
import ratpack.handling.Chain;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;
import ratpack.stream.Streams;
import ratpack.test.exec.ExecHarness;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import static java.util.Arrays.asList;
import static ratpack.func.Function.identity;
import static ratpack.sse.ServerSentEvents.serverSentEvents;


public class MyApp {

    public static void main(String[] args) throws Exception {
        final String seed= "172.29.0.83";
        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoint(seed);
        Cluster cluster= builder.build();
        Session session = cluster.connect("trex");

        ExecHarness.runSingle(e -> {

            Promise<Boolean> aaa = Promise.sync(() -> false);
            Promise bbb = aaa.map(x -> !x);
            bbb.then(x -> System.out.print(x));

        });

        AtomicReference<Promised<String>> ref = new AtomicReference<>(new Promised<>());
        SparkConf conf = new SparkConf(true)
                //.set("spark.local.ip","172.20.0.1") // helps when multiple network interfaces are present. The driver must be in the same network as the master and slaves
                //.set("spark.driver.host","172.20.0.1") // same as above. This duality might disappear in a future version
                .set("spark.cassandra.connection.host", seed)
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

        RatpackServer.start(s -> s
            .serverConfig(c -> c.baseDir(BaseDir.find()))
            .registry(Guice.registry(b -> {
                b.module(MyModule.class);
                //b.bindInstance(SparkSession.class, spark);
                b.add(SparkSession.class, spark);
                b.add(Session.class, session);
            }))
            .handlers(chain -> chain
                .get("pub/:val", ctx -> {
                    String val = ctx.getPathTokens().get("val");
                    ref.getAndSet(new Promised<>()).success(val);
                    ctx.render("published: " + val);
                })
                .get("sub", ctx -> {
                    List<Promise<String>> promises = asList(
                        Promise.value("subscribed"),
                        ref.get().promise().map("received "::concat)
                    );
                    ctx.render(serverSentEvents(
                        Streams.publish(promises).flatMap(identity()),
                        event -> event.event(identity())
                    ));
                })
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
                .path("injected", MyHandler.class) // Map to a dependency injected handler
                .prefix("static",
                        nested -> nested.fileSystem("assets/images", Chain::files)) // Bind the /static app path to the src/ratpack/assets/images dir
                .all(ctx -> ctx.render("root handler!"))
            )
        );
    }
}
