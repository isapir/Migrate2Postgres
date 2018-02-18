package net.twentyonesolutions.m2pg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

public class PgMigrator {

    public final static String DISCL = "Migrate2Postgres Copyright (C) 2018 Igal Sapir\n"
            + "This program comes with ABSOLUTELY NO WARRANTY;\n"
            + "This is free software, and you are welcome to redistribute it\n"
            + "under certain conditions;\n"
            + "See https://www.gnu.org/licenses/gpl-3.0.txt for details\n"
            + "\n"
            + "Available at https://github.com/isapir/Migrate2Postgres";

    public final static String USAGE = "Usage: java <options> "
            + PgMigrator.class.getCanonicalName()
            + " <command> [<config-file> [<output-file>]]\n"
            + "       where command can be:\n"
            + "            ddl - to create the schema objects\n"
            + "            dml - to copy the data\n";


    public static void main(String[] args) throws Exception {

        System.out.println(DISCL + "\n");

        if (args.length == 0){
            System.out.println(USAGE);
            System.exit(-1);
        }

        String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                .withZone(ZoneId.systemDefault())
                .format(Instant.now());

        String configFile = "", outputFile = "";

        if (args.length > 2)
            outputFile = args[2];

        if (args.length > 1)
            configFile = args[1];

        String action = args[0].toLowerCase();

        Config config = Config.fromFile(configFile);

        if (outputFile.isEmpty())
            outputFile = config.name + "-" + action + "-" + timestamp + (action.equals("ddl") ? ".sql" : ".log");

        Schema schema = new Schema(config);

        if (action.equals("ddl")){

            doDdl(schema, outputFile);
        }
        else if (action.equals("dml")){

            doDml(schema, outputFile);
        }
        else {
            System.out.println(USAGE);
            System.exit(-1);
        }
    }


    public static void doDdl(Schema schema, String filename) throws IOException {

        String ddl = schema.generateDdl();

        Path path = Files.write(Paths.get(filename), ddl.getBytes(StandardCharsets.UTF_8));

        System.out.println("Created DDL file at " + path.toAbsolutePath().toString());
    }


    public static void doDml(Schema schema, String filename) throws IOException {
        IProgress progress = new ProgressOneline(schema);

        long tc = System.currentTimeMillis();

        int numThreads = 1;

        Object arg = schema.config.dml.get("threads");

        if (arg != null){

            if (arg instanceof Number){
                numThreads = ((Number) arg).intValue();
            }
            else if (arg instanceof String){
                try {
                    numThreads = Integer.parseInt((String) arg);
                }
                catch (NumberFormatException ex){
                    System.err.println("Failed to parse value of [dml.threads]");
                }
            }
            else {
                System.err.println("[dml.threads] has an invalid value: " + arg.toString());
            }
        }

        System.out.println("Running with " + numThreads + " threads");

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        List<Future<String>> tasks = new ArrayList<>();

        for (String tableName : schema.schema.keySet()){

            Callable<String> callable = () -> {
                String log = schema.copyTable(tableName, progress);
                return log;
            };

            FutureTask<String> task = new FutureTask(callable);
            tasks.add(task);
            executorService.execute(task);
        }

        Path path = Paths.get(filename);

        Files.write(path, Collections.singleton(getBanner()), StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);

        tasks
            .parallelStream()
            .forEach(task -> {
                try {
                    String entry = task.get() + "\n\n";
                    Files.write(path, Collections.singleton(entry), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

        executorService.shutdown();

        tc = System.currentTimeMillis() - tc;

        String logentry = String.format("-- %tT Completed in %.3f seconds\n", System.currentTimeMillis(), tc / 1000.0);

        Files.write(path, Collections.singleton(logentry), StandardCharsets.UTF_8, StandardOpenOption.APPEND);

        System.out.println("\n" + logentry);
        System.out.println("See log and recommended actions at " + path);
    }


    public static String getBanner(){

        return "/**\n"
            + "\tScripted by Migrate2Postgres on "
            + ZonedDateTime.now().format(RFC_1123_DATE_TIME)
            + "\n\n\t"
            + DISCL.replace("\n", "\n\t")
            + "\n**/\n\n";
    }


    static class ProgressOneline implements IProgress {

        final ConcurrentMap<String, Status> processing = new ConcurrentHashMap();
        final ConcurrentMap<String, Table> remaining = new ConcurrentHashMap<>();
        volatile String lastLine = "";
        AtomicLong lastReport = new AtomicLong(System.currentTimeMillis());

        public ProgressOneline(Schema schema) {
            this.remaining.putAll(schema.schema);
        }

        @Override
        public void progress(Status status) {

            if (status.row >= status.rowCount){     // if new records were added to the table while copying then row will be greater than rowCount
//                System.out.println("");
//                System.out.println("processed " + status.name);
                processing.remove(status.name);
                this.remaining.remove(status.name);
                return;
            }


//            Status prev = processing.getOrDefault(status.name, Status.EMPTY);
//            String prevPct = (prev == Status.EMPTY) ? "0.0" : String.format("%.1f", 100.0 * prev.row / prev.rowCount);
//            String currPct = String.format("%.1f", 100.0 * status.row / status.rowCount);

            processing.put(status.name, status);

            if (System.currentTimeMillis() - lastReport.get() < 100)
                return;
            lastReport.set(System.currentTimeMillis());

//            if (!prevPct.equals(currPct)){
                // print status line
                StringBuilder sb = new StringBuilder(512);

                sb.append(remaining.size())
                  .append(" table")
                  .append(remaining.size() > 1 ? "s " : " ")
                  .append("left");

                for (Map.Entry<String, Status> e : processing.entrySet()){

                    sb.append("   ");
                    String name = e.getKey();

                    name = name.substring(name.indexOf(".") + 1);

                    Status s = e.getValue();
                    sb.append(name).append(": ");

                    if (s.rowCount > 10_000_000)
                        sb.append(String.format("%.2f%%", 100.0 * s.row / s.rowCount));
                    else if (s.rowCount > 10_000)
                        sb.append(String.format("%.1f%%", 100.0 * s.row / s.rowCount));
                    else
                        sb.append(String.format("%d%%", 100 * s.row / s.rowCount));
                }

                String statusLine = sb.toString();
                if (!statusLine.equals(lastLine)){

                    System.out.printf("\r%s", statusLine);
                    if (lastLine.length() > statusLine.length())
                        System.out.print("                                                           ");

                    lastLine = statusLine;
                }
//            }
        }
    }

    static class ProgressVerbose implements IProgress {

        String lastReport = "";

        @Override
        public void progress(Status status) {

            String report = String.format("%.1f", 100.0 * status.row / status.rowCount);

            if (!lastReport.equals(report) || status.row == status.rowCount){
                lastReport = report;
                System.out.println(status);
            }
        }
    }

}
