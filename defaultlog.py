# -*- coding: utf-8 -*-

"""Console and file logging configuration.

This module automatically configures the logging to use a colored console
format, and a timed rotating log file that rolls over at midnight.

The log formats results in the following outputs:
    Console:
        [INFO    ] This is some info (root)
        [DEBUG   ] Now this is some debug (parser.ekpo)

    File:
        [2015-12-29T16:16:55]13748 root           INFO   This is some info
        [2015-12-29T16:16:55]13748 parser.ekpo    DEBUG  This is some module debug
        [2015-12-29T16:16:55]13748 root           INFO   Some more info
        
Just put this file into your project structure to use it.

Usage:
    from defaultlog import logging

Examples:
    Package-level using the root logger
    -----------------------------------
    # Import the logging module through this customized module
    from defaultlog import logging

    def main():
        # Log using the root logger
        logging.info('Program started')
        
    if __name__ == '__main__':
        main()

    Library code that log to a private logger
    -----------------------------------------
    # Imports the bult-in logging package
    import logging

    # Creates a private logger instance
    logger = logging.getLogger(__name__)

    def do_it():
        # Log using the private logger
        # It keeps the configuration done in the root logger
        logger.info('Doing it')

Dependencies:
    coloredlogs
    colorama

"""

import logging
import logging.config
import yaml


default_config = \
    """
    version: 1

    formatters: 
        console: 
            format : "[%(levelname)-7s] %(message)s"
            datefmt: "%H:%M:%S"

        file:
            format : "[%(asctime)s]%(thread)-5d %(name)-40s %(levelname)-8s %(message)s"
            datefmt: "%Y-%m-%dT%H:%M:%S"

        colored:
            format : "[%(log_color)s%(levelname)-8s%(reset)s] %(message_log_color)s%(message)s%(reset)s %(name_log_color)s(%(name)s)"
            datefmt: "%H:%M:%S"
            ()     : colorlog.ColoredFormatter

            log_colors:
                DEBUG   : white
                INFO    : bold_green
                WARNING : bold_yellow
                ERROR   : bold_red
                CRITICAL: bold_white,bg_red

            secondary_log_colors:
                message:
                    INFO    : bold_white
                    WARNING : bold_yellow
                    ERROR   : bold_red
                    CRITICAL: bold_red
                name:
                    DEBUG   : purple
                    INFO    : purple
                    WARNING : purple
                    ERROR   : purple
                    CRITICAL: purple

    handlers: 
        console: 
            level    : DEBUG
            class    : logging.StreamHandler
            formatter: colored
            stream   : ext://sys.stdout

        file:
            level    : DEBUG
            class    : logging.handlers.TimedRotatingFileHandler
            formatter: file
            when     : midnight
            filename : logs/log.log
            encoding : utf8

    loggers:
        custom_module:
            handlers: [file]
            level: WARNING

    root: 
        handlers: [console, file]
        level: DEBUG

    disable_existing_loggers: False

    """

try:
    config = yaml.load(default_config)
    logging.config.dictConfig(config)
except Exception:
    logging.exception("Couldn't import logging settings from yaml")
