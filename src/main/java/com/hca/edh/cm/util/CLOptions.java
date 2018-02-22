package com.hca.edh.cm.util;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CLOptions is a helper class to help parse and validate command line options.
 */
public class CLOptions {

    private static final Logger log = LoggerFactory.getLogger(CLOptions.class);
    private String[] args = null;
    private final Options options = new Options();

    public CLOptions(String[] args){
        this.args = args;
        options.addOption("c","configFile",true, "(Required) Absolute path for configuration file");
        options.addOption("f", "flumeConfigFile", true, " (Required) Absolute path for the Flume configuration file.");
    }

    /** Parses and validates the command line options.
     *
     * @return CommandLine
     * @throws ParseException
     */
    public CommandLine parse() throws ParseException {
        CommandLine cmdLine;
        log.debug("Parsing command line arguments...");
        try {
            CommandLineParser parser = new DefaultParser();
            cmdLine = parser.parse(options, args);
            log.debug("Basic parsing is complete.");
            if (!cmdLine.hasOption("c"))
                throw new ParseException("Option 'c' or 'configFile' should be specified.");
            if (!cmdLine.hasOption("f"))
                throw new ParseException("Option 'f' or 'flumeConfigFile' should be specified.");
            log.debug("Successfully parsed all the arguments.");
        }catch (ParseException pe){
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp(pe.getMessage(), options);
            log.error("Exception occurred while parsing command line arguments.", pe);
            throw new ParseException("Failed to parse arguments.");
        }
        return cmdLine;
    }
}