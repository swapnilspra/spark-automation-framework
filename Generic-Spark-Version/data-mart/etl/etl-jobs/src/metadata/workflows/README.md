# Notes #
- workflows are named by file name
- root element of workflow file doesn't matter, it is removed
- only add 1 workflow per yaml file
- workflow files must be named with extension '.yaml'
- job element within task should include project + job name
- schedule_interval uses utc time, so adjust accordingly for chicago time
- type = ON_DEMAND or SCHEDULED, if SCHEDULED include schedule_interval field under workflow
- config file adapted from https://github.com/ajbosco/dag-factory
