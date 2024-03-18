package org.baylight.redis;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class RedisServiceOptions {
    private int port = RedisConstants.DEFAULT_PORT;
    private String role = RedisConstants.LEADER;
    private String replicaof = null;
    private int replicaofPort = RedisConstants.DEFAULT_PORT;
    private String dir = ".";
    private String dbfilename = null;

    public boolean parseArgs(String[] args) {
        // Define the options
        Options options = new Options();

        Option portOption = Option.builder().longOpt("port").hasArg(true)
                .desc("The port number to use").required(false) // Make this option optional
                .type(Number.class).build();
        options.addOption(portOption);

        Option replicaofOption = Option.builder().longOpt("replicaof").numberOfArgs(2)
                .desc("The host and port of the replica").required(false) // Make this option
                                                                          // optional
                .build();
        options.addOption(replicaofOption);

        Option dirOption = Option.builder().longOpt("dir").hasArg(true)
                .desc("The directory where RDB files are stored").required(false) // Make this
                                                                                  // option optional
                .build();
        options.addOption(dirOption);

        Option dbfilenameOption = Option.builder().longOpt("dbfilename").hasArg(true)
                .valueSeparator(' ').desc("The name of the RDB file").required(false) // Make this
                                                                                      // option
                                                                                      // optional
                .build();
        options.addOption(dbfilenameOption);

        // Create a parser and parse the command line arguments
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            // Check if the port option was provided
            if (cmd.hasOption("port")) {
                port = Integer.parseInt(cmd.getOptionValue("port"));
                System.out.println("Port specified: " + getPort());
                if (port <= 0 || port > 65535) {
                    throw new ParseException("Port must be less than or equal to 65535: " + port);
                }
            } else {
                System.out.println("No port specified, using default.");
            }

            if (cmd.hasOption("replicaof")) {
                String[] replicaofStrings = cmd.getOptionValues("replicaof");
                replicaof = replicaofStrings[0];
                replicaofPort = Integer.parseInt(replicaofStrings[1]);
                System.out.println(
                        "Replicaof specified: " + getReplicaof() + " " + getReplicaofPort());
                if (replicaofPort <= 0 || replicaofPort > 65535) {
                    throw new ParseException(
                            "Port must be less than or equal to 65535: " + replicaofPort);
                }

                role = RedisConstants.FOLLOWER;
            }

            if (cmd.hasOption("dir")) {
                dir = cmd.getOptionValue("dir");
                System.out.println("Dir specified: " + getDir());
                // check if dir is a valid directory
                File dirFile = new File(getDir());
                if (!dirFile.isDirectory()) {
                    throw new ParseException("Invalid directory: " + getDir());
                }
            }

            if (cmd.hasOption("dbfilename")) {
                dbfilename = cmd.getOptionValue("dbfilename");
                System.out.println("Dbfilename specified: " + getDbfilename());
            }

        } catch (ParseException e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());

            // Automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("RedisServiceOptions", options);
            return false;
        }
        return true;
    }

    public String getConfigValue(String config) {
        return switch (config) {
        case "port" -> String.valueOf(port);
        case "role" -> role;
        case "replicaof" -> replicaof + " " + replicaofPort;
        case "dir" -> dir;
        case "dbfilename" -> dbfilename;
        default -> null;
        };
    }

    public int getPort() {
        return port;

    }

    public String getRole() {
        return role;
    }

    public String getReplicaof() {
        return replicaof;
    }

    public int getReplicaofPort() {
        return replicaofPort;
    }

    public String getDir() {
        return dir;
    }

    public String getDbfilename() {
        return dbfilename;
    }

}
