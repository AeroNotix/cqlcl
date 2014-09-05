(defpackage :cqlcl
  (:use #:cl #:uuid #:split-sequence)
  (:export
   #:+consistency-digit-to-name+
   #:+consistency-name-to-digit+
   #:conn-options
   #:encode-value
   #:make-connection
   #:make-ipv4
   #:make-ipv6
   #:make-stream-from-byte-vector
   #:parse-boolean
   #:parse-bytes
   #:parse-consistency
   #:parse-int
   #:parse-ip
   #:parse-short
   #:parse-packet
   #:write-short
   #:write-int
   #:parse-short-bytes
   #:parse-string-map
   #:parse-string
   #:parse-uuid))
