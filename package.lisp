(defpackage :cqlcl
  (:use #:cl #:uuid #:split-sequence)
  (:export
   ;; API
   #:make-connection
   #:query

   ;; Exported for tests
   #:ip=
   #:+consistency-digit-to-name+
   #:+consistency-name-to-digit+
   #:conn-options
   #:encode-value
   #:make-ipv4
   #:make-ipv6
   #:make-stream-from-byte-vector
   #:parse-boolean
   #:parse-bytes
   #:parse-consistency
   #:parse-int
   #:parse-ip
   #:parse-short
   #:parse-short-bytes
   #:parse-string
   #:parse-string-map
   #:parse-uuid
   #:read-single-packet
   #:write-int
   #:write-short))
