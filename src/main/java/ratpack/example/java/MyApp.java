package ratpack.example.java;

import com.datastax.driver.core.Cluster;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.error.ServerErrorHandler;
import ratpack.guice.Guice;
import ratpack.handling.Chain;
import ratpack.handling.Context;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;
/*
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
*/
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

//import java.util.concurrent.Flow;
//import java.util.concurrent.SubmissionPublisher;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class MyApp {
    static Logger logger = LoggerFactory.getLogger(MyApp.class.getName());

    static class Todo {
        private String name;

        public Todo(String name){
            this.name=name;
        }

        @Override
        public String toString() {
            return name;
        }

        public static List<Todo> getTodos(){

            return Arrays.asList(new Todo("eins"), new Todo("zwei"));
        }
    }
    static class ComputeFunction {
        public static void compute(Integer v) {
            try {
                logger.debug("compute integer v: " + v);
                Thread.sleep(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public static void compute(String v) {
            try {
                logger.debug("compute String v: " + v);
                Thread.sleep(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        public static void compute(Todo v) {
            try {
                logger.debug("compute Todo v: " + v);
                Thread.sleep(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

/*
    public static class EventListener {

        int count = 0;
        FluxSink<String> sink;

        void generate() {
            while (count < 1000) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                sink.next("event " + count);
                count++;
            }
            count++;
        }

        public void register(FluxSink<String> sink) {
            this.sink = sink;
        }
    }
*/
    public static void main(String[] args) throws Exception {
        logger.info("XX\nXXXXX");
/*
        List<String> items = List.of("1", "2", "3", "4", "5", "6", "7", "8", "9");
        SubmissionPublisher<String> publisher = new SubmissionPublisher<>();
        publisher.subscribe(new MySubscriber<>());

        items.forEach(s -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            publisher.submit(s);
        });

        publisher.close();

        List<Optional<String>> listOfOptionals = Arrays.asList(
                Optional.empty(), Optional.of("foo"), Optional.empty(), Optional.of("bar"));

        List<String> filteredList = listOfOptionals.stream()
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());

        List<String> filteredList2 = listOfOptionals.stream()
                .flatMap(Optional::stream)
                .collect(Collectors.toList());


        Flux<String> dynamicFlux = Flux.create(sink -> {
            EventListener eventListener = new EventListener();
            eventListener.register(sink);
            eventListener.generate();
        });

        Flux.range(1, 1000)
                .parallel(8)
                .runOn(Schedulers.parallel())
                .subscribe(i -> System.out.println(i));

        dynamicFlux.publishOn(Schedulers.parallel()).parallel(8);

        dynamicFlux.subscribe(System.out::println
            ,error -> System.err.println("Error " + error)
            ,() -> System.out.println("Done")
        //    , sub -> sub.request(13)
        );

        Flux<Integer> ints = Flux.range(1, 14);
        ints.subscribe(i -> System.out.println(i),
                error -> System.err.println("Error " + error),
                () -> System.out.println("Done"),
                sub -> sub.request(100));
*/

/*
        Observable.range(1, 1_000_000)
                .observeOn(Schedulers.computation())
                .subscribe(ComputeFunction::compute);
*/
        PublishSubject<Integer> source = PublishSubject.<Integer>create();

        source.observeOn(Schedulers.computation())
                .subscribe(ComputeFunction::compute, Throwable::printStackTrace);

        IntStream.range(1, 1_000).forEach(source::onNext);
        Observable.fromFuture(CompletableFuture.completedFuture("test"))
        .subscribe(ComputeFunction::compute);

        Observable<Todo> vodo= Observable.create(emitter -> {
            try {
                List<Todo> todos = Todo.getTodos();
                for (Todo todo : todos) {
                    emitter.onNext(todo);
                }
                emitter.onComplete();
            } catch (Exception e) {
                emitter.onError(e);
            }
        });
        vodo.subscribeOn(Schedulers.single()).subscribe(todo -> System.out.println(todo.toString()));
        String test= System.getenv("CASSANDRA_SEED");
        final String seed= (test!=null && !test.isEmpty())?test:"localhost";
        Cluster.Builder builder = Cluster.builder();
        builder.withCredentials("trex".trim(), "trex".trim());
        builder.addContactPoint(seed);
        Cluster cluster= builder.build();

        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", seed)
                .set("spark.cassandra.connection.port", "9042")
                .set("spark.cassandra.auth.username", "trex")
                .set("spark.cassandra.auth.password", "trex")
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
/*
    public static class MySubscriber<T> implements Flow.Subscriber<T> {

        private Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            this.subscription.request(1); //  Ask for initial one data object.
        }

        @Override
        public void onNext(T item) {
            System.out.println(item); // Print it.
            subscription.request(1); // Ask for one more.
        }

        @Override
        public void onError(Throwable throwable) {
            throwable.printStackTrace();
        }

        @Override
        public void onComplete() {
            System.out.println("DONE"); // Done with the stream of data.
        }
    }
*/
}
