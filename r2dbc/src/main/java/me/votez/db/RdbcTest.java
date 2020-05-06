package me.votez.db;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Random;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class RdbcTest {
    private final Random random = new Random();
    private final String host;
    private final String user;
    private final String password;
    public static final Logger LOGGER = LoggerFactory.getLogger("me.votez.db.race.R2DBC");

    public RdbcTest(String host, String user, String password) {

        this.host = host;
        this.user = user;
        this.password = password;
    }

    public void test(int concurrency, int executions, int poolSize) throws InterruptedException {

        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, host)
                .option(PORT, 5432)  // optional, defaults to 5432
                .option(USER, user)
                .option(PASSWORD, password)
                .option(DATABASE, "postgres")  // optional
                .option(SSL, false)  // optional
//                .option(OPTIONS, options) // optional
                .build());
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxIdleTime(Duration.ofMillis(1000))
                .maxSize(poolSize)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        int chunk = executions / 10;
        LOGGER.info("Go R2DBC");
        Flux.range(1, executions)
                .doOnNext(i -> {
                    if (i % chunk == 0) LOGGER.info("Processing {}", i);
                })
                .flatMap(Mono::just)
                .flatMap(i -> pool.create(), concurrency)
//                .doOnNext(unused -> System.out.println("Connection created"))
                .doOnError(Throwable::printStackTrace)
                .flatMap(
                        connection ->
                                Mono.from(connection.createStatement(
                                        "SELECT pg_sleep(0.1), title FROM nicer_but_slower_film_list WHERE FID = $1"
//                                        "SELECT * FROM nicer_but_slower_film_list WHERE actors @@ to_tsquery($1)"
                                )
//                                        .bind("$1", "BetteNicholson&WarrenNolte")
                                        .bind("$1", Math.abs(random.nextInt(990)))
                                        .execute())
                                        .doOnError(Throwable::printStackTrace)
                                        .flatMapMany(result -> Flux.from(result.map((row, metadata) -> row.get("TITLE", String.class).length())))
                                        .doOnError(Throwable::printStackTrace)
//                                        .doOnNext(System.out::println)
                                        .then(Mono.from(connection.close()))
                                        .thenReturn("Good")
                )
                .subscribeOn(Schedulers.parallel())
                .publishOn(Schedulers.parallel())
                .doOnError(Throwable::printStackTrace)
//                .subscribe(System.out::println, Throwable::printStackTrace);
                .blockLast();
        Thread.sleep(10_000);
        LOGGER.info("Done with R2DBC");
        pool.close();
        LOGGER.info("Pool is down");
    }
}
