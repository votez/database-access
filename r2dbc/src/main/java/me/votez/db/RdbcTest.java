package me.votez.db;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class RdbcTest {
    public void test(int concurrency, int executions, int poolSize) throws InterruptedException {

        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, "192.168.1.42")
                .option(PORT, 5432)  // optional, defaults to 5432
                .option(USER, "netflix")
                .option(PASSWORD, "R%9jqlyjP1SPqRe7J5s")
                .option(DATABASE, "postgres")  // optional
//                .option(OPTIONS, options) // optional
                .build());
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxIdleTime(Duration.ofMillis(1000))
                .maxSize(poolSize)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        System.out.println("Go R2DBC");
        Flux.range(1, executions)
                .doOnNext(i -> {
                    if (i % 100 == 0) System.out.println("Processing " + i);
                })
                .flatMap(Mono::just, concurrency)
                .flatMap(i -> pool.create())
//                .doOnNext(unused -> System.out.println("Connection created"))
                .doOnError(Throwable::printStackTrace)
                .flatMap(
                        connection ->
                                Mono.from(connection.createStatement("SELECT * FROM nicer_but_slower_film_list WHERE actors @@ to_tsquery($1)")
                                        .bind("$1", "BetteNicholson&WarrenNolte")
                                        .execute())
                                        .doOnError(Throwable::printStackTrace)
                                        .flatMapMany(result -> Flux.from(result.map((row, metadata) -> row.get("TITLE", String.class).length())))
                                        .doOnError(Throwable::printStackTrace)
//                                        .doOnNext(System.out::println)
                                        .then(Mono.from(connection.close()))
                                        .thenReturn("Good")
                )
                .subscribeOn(Schedulers.parallel())
                .doOnError(Throwable::printStackTrace)
//                .subscribe(System.out::println, Throwable::printStackTrace);
                .blockLast();
        Thread.sleep(10_000);
        System.out.println("Done with R2DBC");
        pool.close();
    }
}
