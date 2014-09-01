(defpackage :cqlcl
  (:use #:cl #:uuid #:split-sequence)
  (:export #:parse-packet #:make-connection #:conn-options
           #:encode-value #:make-stream-from-byte-vector
           #:parse-string-map))
