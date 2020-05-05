package me.votez.db;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    public static final Logger LOGGER = LoggerFactory.getLogger("me.votez.db.race.VERTX");

    public static void main( String[] args ) throws InterruptedException, ParseException {
        var options = new Options();
        options.addOption("concurrent", true, "Concurrency");
        options.addOption("iterations", true, "Executions");
        options.addOption("pool", true, "Pool size");
        options.addOption("user", true, "user");
        options.addOption("password", true, "password");
        options.addOption("host", true, "host");
        var res = new DefaultParser().parse(options, args);
        var user = res.getOptionValue("user");
        var password = res.getOptionValue("password");
        var host = res.getOptionValue("host");
        var executions = res.getOptionValue("iterations", "100000");
        var concurrency = res.getOptionValue("concurrent", "100");
        var pool = res.getOptionValue("pool", "20");
        LOGGER.info("Run {}@{} with concurrency: {}, executions {}, pool size {}"
                , user, host, concurrency, executions, pool);
        new VertxTest(host, user, password).test(Integer.parseInt(concurrency), Integer.parseInt(executions), Integer.parseInt(pool));
        System.out.println( "Hello World!" );
    }
}
