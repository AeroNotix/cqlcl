(defpackage :cqlcl
  (:use #:cl #:uuid #:split-sequence)
  (:export #:parse-packet #:make-connection #:conn-options))
