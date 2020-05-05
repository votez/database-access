package me.votez.db;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException, ParseException {
        var options = new Options();
        options.addOption("c","Concurrency");
        options.addOption("e","Executions");
        options.addOption("p","Pool size");
        var res = new DefaultParser().parse(options,args);
        var executions = res.getOptionValue("e", "100000");
        var concurrency = res.getOptionValue("c", "100");
        var pool = res.getOptionValue("p", "20");
        System.out.printf("Run with concurrency: %s, executions %s, pool size %s\n",concurrency, executions, pool);
        new VertxTest().test(Integer.parseInt(concurrency), Integer.parseInt(executions), Integer.parseInt(pool));
        System.out.println( "Hello World!" );
    }
}
