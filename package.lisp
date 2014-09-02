(defpackage :cqlcl
  (:use #:cl #:uuid #:split-sequence)
  (:export
   #:+consistency-digit-to-name+
   #:+consistency-name-to-digit+
   #:conn-options
   #:encode-value
   #:make-connection
   #:make-stream-from-byte-vector
   #:parse-bytes
   #:parse-consistency
   #:parse-packet
   #:parse-short-bytes
   #:parse-string-map
   #:parse-uuid))
