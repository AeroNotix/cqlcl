cqlcl
=====

A Cassandra native protocol client in Erlang


Installation
------------

```shell

cd $QUICKLISP_HOME/.local-projects/
git clone git@github.com:AeroNotix/cqlcl.git
sbcl --eval "(ql:quickload :cqlcl)"
```

This will pull in all required dependencies.

Status
------

Currently only supports connecting and sending the OPTIONS request.
