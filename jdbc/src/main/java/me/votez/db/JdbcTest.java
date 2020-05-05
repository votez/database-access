package me.votez.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;


public class JdbcTest {
    public void test(int concurrency, int executions, int poolSize) throws InterruptedException, SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://192.168.1.42/postgres");
        config.setUsername("netflix");
        config.setPassword("R%9jqlyjP1SPqRe7J5s");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMaximumPoolSize(poolSize);

        HikariDataSource ds = new HikariDataSource(config);

        System.out.println("Go JDBC");
        Flowable.range(1, executions)
                .doOnNext(i -> {if (i % 100 == 0) System.out.println("Process " + i);})
                .flatMapSingle(Single::just,false, concurrency)
                .flatMapSingle( unused -> Single.fromCallable(ds::getConnection)
                        .map( connection -> {
                            var s = connection.prepareStatement("SELECT * FROM nicer_but_slower_film_list WHERE actors @@ to_tsquery(?)");
                            s.setString(1, "BetteNicholson&WarrenNolte");
                            var results = s.executeQuery();
                            int counter = 0;
                            while(results.next()){
                                counter += results.getString("title").length();
                            }
                            results.close();
                            s.close();
                            connection.close();
                            return counter;
                        }).subscribeOn(Schedulers.io())
                )
                .subscribeOn(Schedulers.io())
                .blockingSubscribe(unused -> {}, Throwable::printStackTrace, ds::close);

        System.out.println("Done with JDBC");

    }
}
