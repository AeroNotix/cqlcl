LISP ?= sbcl
SOURCES := $(wildcard *.lisp)
sbcl_TEST_OPTS=--noinform --disable-debugger --quit --load ./run-tests.lisp

.PHONY: test

test:
	@$(LISP) $($(LISP)_TEST_OPTS)
