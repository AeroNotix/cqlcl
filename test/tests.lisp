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
      (assert-true (uuid:uuid= parsed u)))))

(define-test encode-decode-consistency
  (maphash (lambda (k v)
             (declare (ignore v))
             (let* ((os (flexi-streams:make-in-memory-output-stream))
                    (ims (flexi-streams:make-flexi-stream os)))
               (encode-value k ims)
               (let* ((bv (flexi-streams:get-output-stream-sequence os))
                      (is (make-stream-from-byte-vector bv))
                      (parsed (parse-consistency is)))
                 (assert-equalp parsed k))))
           +consistency-name-to-digit+))

(define-test encode-decode-bytes
    (let* ((n 65536)
           (bv (make-array n :fill-pointer 0 :element-type '(unsigned-byte 8) :adjustable t))
           (os (flexi-streams:make-in-memory-output-stream))
           (ims (flexi-streams:make-flexi-stream os)))
      (dotimes (x n)
        (vector-push-extend (random 128) bv))
      (encode-value bv ims)
      (let* ((sbv (flexi-streams:get-output-stream-sequence os))
             (is (make-stream-from-byte-vector sbv))
             (parsed (parse-bytes is)))
        (assert-equalp parsed bv))))

(define-test encode-decode-short-bytes
    (let* ((n 65535)
           (bv (make-array n :fill-pointer 0 :element-type '(unsigned-byte 8) :adjustable t))
           (os (flexi-streams:make-in-memory-output-stream))
           (ims (flexi-streams:make-flexi-stream os)))
      (dotimes (x n)
        (vector-push-extend (random 128) bv))
      (encode-value bv ims)
      (let* ((sbv (flexi-streams:get-output-stream-sequence os))
             (is (make-stream-from-byte-vector sbv))
             (parsed (parse-short-bytes is)))
        (assert-equalp parsed bv))))

(define-test encode-decode-long-bytes
    (let* ((n 65536)
           (bv (make-array n :fill-pointer 0 :element-type '(unsigned-byte 8) :adjustable t))
           (os (flexi-streams:make-in-memory-output-stream))
           (ims (flexi-streams:make-flexi-stream os)))
      (dotimes (x n)
        (vector-push-extend (random 128) bv))
      (encode-value bv ims)
      (let* ((sbv (flexi-streams:get-output-stream-sequence os))
             (is (make-stream-from-byte-vector sbv))
             (parsed (parse-bytes is)))
        (assert-equalp parsed bv))))

(define-test encode-decode-short
    (loop for i from 0 to 65535
         do
         (let* ((os (flexi-streams:make-in-memory-output-stream))
                (ims (flexi-streams:make-flexi-stream os)))
           (write-short i ims)
           (let* ((bv (flexi-streams:get-output-stream-sequence os))
                  (is (make-stream-from-byte-vector bv))
                  (parsed (parse-short is)))
             (assert-equalp parsed i)))))

(define-test encode-decode-int

    (loop for i from -65535 to 65535
         do
         (let* ((os (flexi-streams:make-in-memory-output-stream))
                (ims (flexi-streams:make-flexi-stream os)))
           (write-int i ims)
           (let* ((bv (flexi-streams:get-output-stream-sequence os))
                  (is (make-stream-from-byte-vector bv))
                  (parsed (parse-int is)))
             (assert-equalp parsed i)))))

(define-test encode-decode-boolean
    (dolist (el '(t nil))
      (let* ((os (flexi-streams:make-in-memory-output-stream))
             (ims (flexi-streams:make-flexi-stream os)))
        (encode-value el ims)
        (let* ((bv (flexi-streams:get-output-stream-sequence os))
               (is (make-stream-from-byte-vector bv))
               (parsed (parse-boolean is)))
          (assert-equalp parsed el)))))

(define-test encode-decode-ip
    ;; TODO: Fix this, implement something to compare ips
    (let ((ip4 (make-ipv4))
          (ip6 (make-ipv6)))
      (dolist (el (list ip4 ip6))
        (let* ((os (flexi-streams:make-in-memory-output-stream))
               (ims (flexi-streams:make-flexi-stream os)))
          (encode-value el ims)
          (force-output ims)
          (let* ((bv (flexi-streams:get-output-stream-sequence os))
                 (is (make-stream-from-byte-vector bv))
                 (parsed (parse-ip is)))
            (assert-equalp parsed el))))))
