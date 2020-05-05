package me.votez.db;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.internal.functions.Functions;
import io.reactivex.schedulers.Schedulers;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.reactivex.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;

import java.util.Random;

public class VertxTest {

    private final Random random = new Random();

    public void test(int concurrency, int executions, int pool) {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(5432)
                .setHost("192.168.1.42")
                .setDatabase("postgres")
                .setUser("netflix")
                .setPassword("R%9jqlyjP1SPqRe7J5s");

// Pool options
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(pool);

// Create the client pool
        PgPool client = PgPool.pool(connectOptions, poolOptions);

        System.out.println("Go Vert.X");
        Flowable.range(1, executions)
                .doOnNext(i -> {if (i % 100 == 0) System.out.println("Process " + i);})
                .flatMapSingle(Single::just,false, concurrency)
                .flatMapSingle( i->  client.preparedQuery("SELECT * FROM nicer_but_slower_film_list WHERE actors @@ to_tsquery($1)").rxExecute(Tuple.of("BetteNicholson&WarrenNolte")))
//                    return  client.preparedQuery("SELECT title FROM nicer_but_slower_film_list WHERE FID = $1").rxExecute(Tuple.of(random.nextInt(990)));
                .flatMapIterable(Functions.identity())
                .map(row -> row.getString("title").length())
                .doOnError(Throwable::printStackTrace)
                .subscribeOn(Schedulers.computation())
                .blockingSubscribe(unused -> {}, Throwable::printStackTrace, client::close);

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

        System.out.println("Done with VertX");
    }
}
