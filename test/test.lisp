(defpackage #:cqlcql-test
  (:use :cl :cqlcl :lisp-unit))

(in-package #:cqlcl-test)

(define-test one-plus-one
    (assert-equal (+ 1 1) 2))
