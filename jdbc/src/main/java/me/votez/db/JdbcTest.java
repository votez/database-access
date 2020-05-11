package me.votez.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;


public class JdbcTest {
    private final Random random = new Random();
    private final String host;
    private final String user;
    private final String password;
    public static final Logger LOGGER = LoggerFactory.getLogger("me.votez.db.race.JDBC");

    public JdbcTest(String host, String user, String password) {
        this.host = host;
        this.user = user;
        this.password = password;
    }

    public void test(int concurrency, int executions, int poolSize) throws InterruptedException, SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + host + "/postgres");
        config.setUsername(user);
        config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMaximumPoolSize(poolSize);
        config.setConnectionTimeout(180_000);
        config.setAutoCommit(false);
        config.setInitializationFailTimeout(180_000);
        config.setIsolateInternalQueries(false);

        HikariDataSource ds = new HikariDataSource(config);

        LOGGER.info("Go JDBC");
        int chunk = executions / 10;
        Flux.range(1, executions)
                .flatMap(Mono::just)
                .flatMap(index -> Mono.fromCallable(ds::getConnection)
                                .doOnNext(i -> {
                                    if (index % chunk == 0) LOGGER.info("Process {}", index);
                                })
                                .map(this::request).subscribeOn(Schedulers.elastic())
                        , concurrency
                )
                .subscribeOn(Schedulers.elastic())
                .doOnError(Throwable::printStackTrace)
                .doOnComplete( ()->LOGGER.info("Done with JDBC"))
                .blockLast();
        ds.close();
        LOGGER.info("Pool is down");


    }

    private Integer request(Connection connection) {
        try {
            var s = connection.prepareStatement(
                    "SELECT title FROM nicer_but_slower_film_list WHERE FID = ?"
            );
            s.setInt(1, Math.abs(random.nextInt(990)));
            var results = s.executeQuery();
            int counter = 0;
            while (results.next()) {
                counter += results.getString("title").length();
            }
            results.close();
            s.close();
            connection.close();
            return counter;
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }
}
