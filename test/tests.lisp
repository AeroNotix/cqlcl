(defpackage #:cqlcl-test
  (:use :cl :cqlcl :lisp-unit))

(in-package #:cqlcl-test)


(define-test parse-options-header
  (labels ((hash-equal (h1 h2)
             (maphash (lambda (k v)
                        (assert-equal (gethash k h2) (values v t))) h1)))
    (let* ((packet #(129 0 0 6 0 0 0 52 0 2 0 11 67 81 76 95 86 69 82
                     83 73 79 78 0 1 0 5 51 46 49 46 50 0 11 67 79 77
                     80 82 69 83 83 73 79 78 0 2 0 6 115 110 97 112 112
                     121 0 3 108 122 52))
           (expected-hash (alexandria:alist-hash-table
                           '(("CQL_VERSION" . ("3.1.2"))
                             ("COMPRESSION" . ("snappy" "lz4"))) :test #'equalp))
           (parsed (parse-packet packet)))
      (hash-equal parsed expected-hash))))

(define-test simple-connection
  (let* ((conn (make-connection))
         (conopt (gethash "COMPRESSION" (conn-options conn)))
         (expected (values '("snappy" "lz4") t)))
    (assert-equal conopt expected)))

(define-test encode-decode-string-map
  (labels ((hash-equal (h1 h2)
             (maphash (lambda (k v)
                        (assert-equal (gethash k h2) (values v t))) h1)))
    (let* ((smap (alexandria:alist-hash-table
                  '(("KEYNAME" . "KEYVALUE")
                    ("KEYNAME2" . "KEYVALUE2")) :test #'equalp))
           (os (flexi-streams:make-in-memory-output-stream))
           (ims (flexi-streams:make-flexi-stream os)))
      (encode-value smap ims)
      (let* ((bv (flexi-streams:get-output-stream-sequence os))
             (is (make-stream-from-byte-vector bv))
             (parsed (parse-string-map is)))
        (hash-equal parsed smap)))))

(define-test encode-decode-uuid
  (let* ((os (flexi-streams:make-in-memory-output-stream))
         (ims (flexi-streams:make-flexi-stream os))
         (u   (uuid:make-v4-uuid)))
    (encode-value u ims)
    (let* ((bv (flexi-streams:get-output-stream-sequence os))
           (is (make-stream-from-byte-vector bv))
           (parsed (parse-uuid is)))
      (assert-equalp parsed (uuid:uuid-to-byte-array u)))))
