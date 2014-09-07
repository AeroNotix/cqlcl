(defpackage #:cqlcl-test
  (:use :cl :cqlcl :fiveam))

(in-package #:cqlcl-test)
(def-suite :cqlcl)
(in-suite :cqlcl)

(defun hash-equal (h1 h2)
  (maphash (lambda (k v)
             (is (equalp (gethash k h2) (values v t)))) h1))

(test parse-options-header
  (let* ((packet (make-stream-from-byte-vector
                  #(129 0 0 6 0 0 0 52 0 2 0 11 67 81 76 95 86 69 82
                    83 73 79 78 0 1 0 5 51 46 49 46 50 0 11 67 79 77
                    80 82 69 83 83 73 79 78 0 2 0 6 115 110 97 112 112
                    121 0 3 108 122 52)))
         (expected-hash (alexandria:alist-hash-table
                         '(("CQL_VERSION" . ("3.1.2"))
                           ("COMPRESSION" . ("snappy" "lz4"))) :test #'equalp))
         (parsed (read-single-packet packet)))
      (hash-equal parsed expected-hash)))

(test simple-connection
  (let* ((conn (make-connection))
         (conopt (gethash "COMPRESSION" (conn-options conn)))
         (expected (values '("snappy" "lz4") t)))
    (is (equal conopt expected))))

(test encode-decode-string-map
  (let* ((smap (alexandria:alist-hash-table
                '(("KEYNAME" . "KEYVALUE")
                  ("KEYNAME2" . "KEYVALUE2")) :test #'equalp))
         (os (flexi-streams:make-in-memory-output-stream))
         (ims (flexi-streams:make-flexi-stream os)))
    (encode-value smap ims)
    (let* ((bv (flexi-streams:get-output-stream-sequence os))
           (is (make-stream-from-byte-vector bv))
           (parsed (parse-string-map is)))
      (hash-equal parsed smap))))

(test encode-decode-uuid
  (let* ((os (flexi-streams:make-in-memory-output-stream))
         (ims (flexi-streams:make-flexi-stream os))
         (u   (uuid:make-v4-uuid)))
    (encode-value u ims)
    (let* ((bv (flexi-streams:get-output-stream-sequence os))
           (is (make-stream-from-byte-vector bv))
           (parsed (parse-uuid is)))
      (is (uuid:uuid= parsed u)))))

(test encode-decode-consistency
  (maphash (lambda (k v)
             (declare (ignore v))
             (let* ((os (flexi-streams:make-in-memory-output-stream))
                    (ims (flexi-streams:make-flexi-stream os)))
               (encode-value k ims)
               (let* ((bv (flexi-streams:get-output-stream-sequence os))
                      (is (make-stream-from-byte-vector bv))
                      (parsed (parse-consistency is)))
                 (is (equalp parsed k)))))
           +consistency-name-to-digit+))

(test encode-decode-bytes
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
        (is (equalp parsed bv)))))

(test encode-decode-short-bytes
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
        (is (equalp parsed bv)))))

(test encode-decode-long-bytes
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
        (is (equalp parsed bv)))))

(test encode-decode-int
  (loop for i in '(-65535 0 65535)
     do
       (let* ((os (flexi-streams:make-in-memory-output-stream))
              (ims (flexi-streams:make-flexi-stream os)))
         (write-int i ims)
         (let* ((bv (flexi-streams:get-output-stream-sequence os))
                (is (make-stream-from-byte-vector bv))
                (parsed (parse-int is)))
           (is (equalp parsed i))))))

(test encode-decode-boolean
    (dolist (el '(t nil))
      (let* ((os (flexi-streams:make-in-memory-output-stream))
             (ims (flexi-streams:make-flexi-stream os)))
        (encode-value el ims)
        (let* ((bv (flexi-streams:get-output-stream-sequence os))
               (is (make-stream-from-byte-vector bv))
               (parsed (parse-boolean is)))
          (is (equalp parsed el))))))

(test encode-decode-ip
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
            (is (ip= parsed el)))))))

(defun create-keyspace (keyspace)
  (format nil "CREATE KEYSPACE ~A
                          WITH replication = {
                              'class': 'SimpleStrategy', 'replication_factor': '1'
                          }"
          keyspace))

(defun drop-keyspace (keyspace)
  (format nil "DROP KEYSPACE ~A" keyspace))

(defun create-table (table-name)
  (format nil "CREATE TABLE ~A.test (
                          id uuid PRIMARY KEY,
                          name text,
                          value int
                      )"
          table-name))

(defun drop-table (table-name)
  (format nil "DROP TABLE ~A.test" table-name))

(defun random-string ()
  (format nil "~{~A~}"
          (loop for i from 0 upto 10
             collect
               (code-char (+ (random 25) 65)))))

(test create/drop-keyspace
  (let* ((cxn (make-connection))
         (table-name (random-string))
         (create-keyspace (create-keyspace table-name))
         (drop-keyspace (drop-keyspace table-name)))
    (is (equal (query cxn create-keyspace) t))
    (is (equal (query cxn drop-keyspace) t))))

(test create/drop-tables
  (let* ((cxn (make-connection))
         (table-name (random-string))
         (create-keyspace (create-keyspace table-name))
         (drop-keyspace (drop-keyspace table-name))
         (create-table (create-table table-name))
         (drop-table (drop-table table-name)))
    (is (equal (query cxn create-keyspace) t))
    (is (equal (query cxn create-table) t))
    (is (equal (query cxn drop-table) t))
    (is (equal (query cxn drop-keyspace) t))))

(test querying-data
  (let* ((cxn (make-connection))
         (table-name (random-string))
         (create-keyspace (create-keyspace table-name))
         (drop-keyspace (drop-keyspace table-name))
         (create-table (create-table table-name))
         (drop-table (drop-table table-name)))
    (query cxn create-keyspace)
    (query cxn create-table)
    (is (not (query cxn (format nil "SELECT * FROM ~a.test" table-name))))
    (is (not (prepare cxn (format nil "INSERT INTO ~a.test (id, name, value) VALUES(?, ?, ?)" table-name))))
    (is (not (execute cxn (format nil "INSERT INTO ~a.test (id, name, value) VALUES(?, ?, ?)" table-name)
             (uuid:make-v4-uuid) "HELLO" 123)))
    (is (not (execute cxn (format nil "INSERT INTO ~a.test (id, name, value) VALUES(?, ?, ?)" table-name)
             (uuid:make-v4-uuid) "HELLO" 123)))
    (is (not (execute cxn (format nil "INSERT INTO ~a.test (id, name, value) VALUES(?, ?, ?)" table-name)
             (uuid:make-v4-uuid) "HELLO" 123)))
    (is (= (length (query cxn (format nil "SELECT * FROM ~a.test" table-name))) 3))
    (query cxn drop-table)
    (query cxn drop-keyspace)))

(test set-keyspace
  (let* ((cxn (make-connection))
         (table-name (random-string))
         (create-keyspace (create-keyspace table-name))
         (drop-keyspace (drop-keyspace table-name)))
    (query cxn create-keyspace)
    (is (query cxn (format nil "SET KEYSPACE ~A" table-name)))
    (query cxn drop-keyspace)))
