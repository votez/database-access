package me.votez.db;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.internal.functions.Functions;
import io.reactivex.schedulers.Schedulers;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.reactivex.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class VertxTest {
    public static final Logger LOGGER = LoggerFactory.getLogger("me.votez.db.race.VERTX");

    private final Random random = new Random();
    private final String host;
    private final String user;
    private final String password;

    public VertxTest(String host, String user, String password) {

        this.host = host;
        this.user = user;
        this.password = password;
    }

    public void test(int concurrency, int executions, int pool) {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(5432)
                .setHost(host)
                .setDatabase("postgres")
                .setCachePreparedStatements(true)
                .setPreparedStatementCacheMaxSize(1000)
                .setSsl(false)
                .setUser(user)
                .setPassword(password);

// Pool options
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(pool);

// Create the client pool
        PgPool client = PgPool.pool(connectOptions, poolOptions);

        int chunk = executions / 10;
        LOGGER.info("Go Vert.X");
        Flowable.range(1, executions)
                .doOnNext(i -> {if (i % chunk == 0) LOGGER.info("Process {}", i);})
                .flatMap( i-> client.preparedQuery(
                        "SELECT pg_sleep(0.1), title FROM nicer_but_slower_film_list WHERE FID = $1")
//                        "SELECT * FROM nicer_but_slower_film_list WHERE actors @@ to_tsquery($1)")
//                        .rxExecute(Tuple.of("BetteNicholson&WarrenNolte"))
                        .rxExecute(Tuple.of(Math.abs(random.nextInt(990))))
                        .flattenAsFlowable(Functions.identity())
                .map(row -> row.getString("title").length())
                .doOnError(Throwable::printStackTrace)
                .subscribeOn(Schedulers.computation()),
        false, concurrency)
//                    return  client.preparedQuery("SELECT title FROM nicer_but_slower_film_list WHERE FID = $1").rxExecute(Tuple.of(random.nextInt(990)));
                .blockingSubscribe(unused -> {}, Throwable::printStackTrace);

/*
        Flowable.range(1, 1_000_000)
                .flatMapSingle(i -> {
                    if( i % 100 == 0 ) System.out.println("Process " + i);
                    return  client.rxGetConnection();
                }, false, 1000)
                .doOnError(Throwable::printStackTrace)
                .flatMap(connection ->
                        connection.rxPrepare("SELECT * FROM nicer_but_slower_film_list WHERE actors @@ to_tsquery($1)")
                                .flatMap(statement -> statement.query().rxExecute(Tuple.of("BetteNicholson&WarrenNolte")))
                                .flattenAsFlowable(Functions.identity())
                                .flatMapSingle(r -> connection.rxPrepare("SELECT title FROM nicer_but_slower_film_list WHERE FID = $1"))
                                .flatMapSingle(statement -> statement.query().rxExecute(Tuple.of(random.nextInt(990))))
                                .flatMapIterable(Functions.identity())
                                .map(row -> row.getString("title").length())
                                .doOnError(Throwable::printStackTrace)
                                .doOnComplete(connection::close))
                .subscribeOn(Schedulers.computation())
                .blockingSubscribe(unused -> {}, Throwable::printStackTrace, client::close);
*/

        LOGGER.info("Done with VertX");
        client.close();
        LOGGER.info("Pool is down");
    }
}
