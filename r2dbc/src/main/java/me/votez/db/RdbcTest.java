package me.votez.db;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
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

        Flux<String> titles = Flux.usingWhen(pool.create(), connection ->
                        Flux.from(
                                connection.createStatement("SELECT title FROM nicer_but_slower_film_list WHERE FID = $1")
                                        .bind("$1", Math.abs(random.nextInt(990)))
                                        .execute()
                        ).flatMap(result -> result.map(RdbcTest::getTitle))
                , Connection::close);
        titles
                .doOnNext(LOGGER::info)
                .blockLast();

        LOGGER.info("Go R2DBC");
        Flux.range(1, executions)
                .doOnNext(i -> {  if (i % chunk == 0) LOGGER.info("Processing {}", i);})
                .flatMap(i -> Flux.usingWhen(pool.create(),
                        connection -> Flux.from(
                                connection.createStatement("SELECT title FROM nicer_but_slower_film_list WHERE FID = $1")
                                        .bind("$1", Math.abs(random.nextInt(990)))
                                        .execute()
                        )
                                .flatMap(result -> Flux.from(result.map(RdbcTest::getTitle))),
                        Connection::close)
                                .subscribeOn(Schedulers.parallel())
                                .doOnError(Throwable::printStackTrace)
                        , concurrency)
                .doOnError(Throwable::printStackTrace)
                .doOnComplete(() -> LOGGER.info("Done with R2DBC"))
                .blockLast();
        pool.close();
        LOGGER.info("Pool is down");
    }

    private static String getTitle(Row row, RowMetadata meta) {
        return row.get("title", String.class);
    }
}
