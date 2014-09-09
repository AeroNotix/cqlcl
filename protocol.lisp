(in-package :cqlcl)


(defparameter *write-wide-lengths*  nil)
(defparameter *write-short-lengths* nil)
(defparameter *unread-size*         nil)
(defparameter *executing*           nil)


(defun not-implemented (stream)
  (declare (ignore stream))
  (error "Not implemented!"))

(defgeneric encode-value (value stream)
  (:documentation "Encodes a value into the CQL wire format."))

(defmacro as-flags (&rest values)
  `(logior
    ,@(mapcar (lambda (value)
                `(ldb (byte 8 0) ,value)) values)))

(defun encode-values (values stream)
  (let ((*write-wide-lengths* t))
    (mapcar (lambda (value)
              (encode-value value stream)) values)))

(defmethod encode-value :around ((value header) stream)
  ;; TODO: Implement `define-binary-type' which would:
  ;; (define-binary-type header
  ;;  ((version/request-type (flags (0 version)
  ;;                                (1 request-type))
  ;;   (request-flags        (flags (0 compression)
  ;;                                (1 tracing))
  ;;   (stream-id            octet)
  ;;   (length               octet))
  ;;
  ;; And that would generate a `DEFCLASS' to hold the values as well
  ;; as the requisite parsing code (as below).
  (let* ((os (flexi-streams:make-in-memory-output-stream))
         (ims (flexi-streams:make-flexi-stream os)))
    (write-octet (as-flags (ptype value) (vsn value)) stream)
    (write-octet (as-flags (if (compression value) 1 0)
                           (if (tracing value) 1 0)) stream)
    (write-octet (id value) stream)
    (write-octet (gethash (op value) +op-code-name-to-digit+) stream)
    (call-next-method value ims)
    (force-output ims)
    (let ((bv (flexi-streams:get-output-stream-sequence os)))
      (write-int (length bv) stream)
      (write-sequence bv stream))
    (force-output stream)))

(defmethod encode-value ((header options-header) stream)
  (declare (ignore header stream)))

(defmethod encode-value ((header startup-header) stream)
  (encode-value (opts header) stream))

(defmethod encode-value ((header query-header) stream)
  (let ((c (gethash (consistency header) +consistency-name-to-digit+)))
    (write-int (length (qs header)) stream)
    (write-sequence (as-bytes (qs header)) stream)
    (write-short c stream)))

(defmethod encode-value ((header prepare-header) stream)
  (let ((c (gethash (consistency header) +consistency-name-to-digit+)))
    (write-int (length (ps header)) stream)
    (write-sequence (as-bytes (ps header)) stream)
    (write-short c stream)))

(defmethod encode-value ((header execute-header) stream)
  (let ((c (gethash (consistency header) +consistency-name-to-digit+))
        (*executing* t)
        (qid (qid header)))
    (encode-value qid stream)
    (write-short (length (vals header)) stream)
    (encode-values (vals header) stream)
    (write-short c stream)))

(defun write-length (thing stream)
  (let ((len (length thing)))
    (if (or (> len 65535) *write-wide-lengths*)
        (write-int len stream)
        (write-short len stream))
    len))

(defmethod encode-value ((values list) stream)
  (let* ((*write-wide-lengths* nil)
         (*write-short-lengths* t)
         (os (flexi-streams:make-in-memory-output-stream))
         (ims (flexi-streams:make-flexi-stream os)))
    (write-short (length values) ims)
    (mapcar (lambda (value)
              (encode-value value ims)) values)
    (force-output ims)
    (let ((bv (flexi-streams:get-output-stream-sequence os)))
      (write-int (length bv) stream)
      (write-sequence bv stream))))

(defmethod encode-value ((value integer) stream)
  (if *write-short-lengths*
      (write-short 4 stream)
      (write-int 4 stream))
  (write-int value stream))

(defmethod encode-value ((value varint) stream)
  (let* ((v (val value))
         (byte-size (min-bytes v)))
    (write-int byte-size stream)
    (write-sized v (* byte-size 8) stream)))

(defmethod encode-value ((value bigint) stream)
  (write-int 8 stream)
  (write-bigint (val value) stream))

(defmethod encode-value ((value string) stream)
  (encode-value (as-bytes value) stream))

(defmethod encode-value ((value hash-table) stream)
  (let* ((num-entries (hash-table-count value))
         (*write-wide-lengths* nil)
         (*write-short-lengths* t)
         (os (flexi-streams:make-in-memory-output-stream))
         (ims (flexi-streams:make-flexi-stream os)))
    (write-short num-entries ims)
    (maphash (lambda (k v)
               (encode-value k ims)
               (encode-value v ims)) value)
    (let ((bv (flexi-streams:get-output-stream-sequence os)))
      (when *executing*
        (write-int (length bv) stream))
      (write-sequence bv stream))))

(defmethod encode-value ((value null) stream)
  (when *write-wide-lengths*
    (write-int 1 stream))
  (write-octet 0 stream))

(defmethod encode-value ((value vector) stream)
  (write-length value stream)
  (write-sequence value stream))

(defmethod encode-value ((value symbol) stream)
  (let ((consistency (gethash value +consistency-name-to-digit+)))
    (when *write-wide-lengths*
      (write-int 1 stream))
    (cond
      ((eq t value)
       (write-octet 1 stream))
      (consistency
       (write-short consistency stream))
      (t
       (error (format nil "Unknown symbol or keyword attempted to be encoded: ~A"
                      value))))))

(defmethod encode-value ((value uuid) stream)
  (when *write-wide-lengths*
    (write-int 16 stream))
  (write-sequence (uuid-to-byte-array value) stream))

(defmethod encode-value ((value ipv4) stream)
  (let ((encoded-ip (ip-to-integer value)))
    (if *write-wide-lengths*
        (write-int 4 stream)
        (write-octet 4 stream))
    (write-int encoded-ip stream)))

(defmethod encode-value ((value ipv6) stream)
  (let ((encoded-ip (ip-to-integer value)))
    (if *write-wide-lengths*
        (write-int 16 stream)
        (write-octet 16 stream))
    (write-ipv6 encoded-ip stream)))

(defun as-string (bv)
  (flexi-streams:octets-to-string bv :external-format :utf-8))

(defun as-bytes (s)
  (flexi-streams:string-to-octets s))

(defun parse-bytes* (stream size &optional (post-process #'identity))
  (let* ((size (max size 0))
         (buf  (make-array size :element-type '(unsigned-byte 8))))
    (assert (= (read-sequence buf stream :end size) size))
    (funcall post-process buf)))

(defun parse-boolean (stream &optional size)
  (declare (ignore size))
  (when *unread-size*
    (read-short stream))
  (let ((b (read-byte stream)))
    (not (zerop b))))

(defun parse-uuid (stream &optional (size 16))
  (when *unread-size*
    (read-short stream))
  (let ((bytes (parse-bytes* stream size)))
    (uuid:byte-array-to-uuid bytes)))

(defun parse-ip (stream &optional (size (read-octet stream)))
  (assert (or (= size 4)
              (= size 16)))
  (let ((ip-bytes (parse-bytes* stream size)))
    (byte-array-to-ip ip-bytes)))

(defun parse-int (stream &optional size)
  (declare (ignore size))
  (read-int stream :signed? t))

(defun parse-varint (stream &optional size)
  (read-sized (* (or size (parse-short stream)) 8) stream t))

(defun parse-timestamp (stream &optional size)
  (read-bigint stream))

(defun parse-bigint (stream &optional size)
  (declare (ignore size))
  (read-bigint stream :signed? t))

(defun parse-short (stream &optional size)
  (declare (ignore size))
  (read-short stream))

(defun parse-bytes (stream &optional (size (read-int stream)))
  (parse-bytes* stream size))

(defun parse-short-bytes (stream &optional (size (read-short stream)))
  (parse-bytes* stream size))

(defun parse-consistency (stream)
  (gethash (read-short stream) +consistency-digit-to-name+))

(defun parse-string (stream &optional (size (read-short stream)))
  (parse-bytes* stream size #'as-string))

(defun parse-string-list (stream)
  (let* ((size (read-short stream)))
    (loop for i from 1 upto size
       collect
         (parse-string stream))))

(defun make-parse-coll (value-fn)
  (lambda (stream &rest size)
    (declare (ignore size))
    (let ((num-entries (read-short stream))
          (coll nil))
      (dotimes (i num-entries)
        (push (funcall value-fn stream) coll))
      (reverse coll))))

(defun parse-map (stream value-fn)
  (let ((map (make-hash-table :test #'equalp))
        (num-entries (read-short stream))
        (*unread-size* t))
    (dotimes (i num-entries)
      (destructuring-bind (key entry) (funcall value-fn stream)
        (setf (gethash key map) entry)))
    map))

(defun make-parse-map (value-fn)
  (lambda (stream &rest size)
    (declare (ignore size))
    (parse-map stream value-fn)))

(defun parse-string-multimap (stream)
  (parse-map stream (juxt #'parse-string #'parse-string-list)))

(defun parse-string-map (stream)
  (parse-map stream (juxt #'parse-string #'parse-string)))

(defun parse-option-list (stream)
  (loop for x upto (read-short stream)
     collect
       (parse-option stream)))

(defparameter +option-id-to-parser+
  (let ((funs
         `((:custom    . ,#'not-implemented)
           (:ascii     . ,#'parse-string)
           (:bigint    . ,#'parse-bigint)
           (:blob      . ,#'parse-string)
           (:boolean   . ,#'parse-boolean)
           (:counter   . ,#'not-implemented)
           (:decimal   . ,#'not-implemented)
           (:double    . ,#'not-implemented)
           (:float     . ,#'not-implemented)
           (:int       . ,#'parse-int)
           (:text      . ,#'parse-string)
           (:timestamp . ,#'parse-timestamp)
           (:uuid      . ,#'parse-uuid)
           (:varchar   . ,#'parse-string)
           (:varint    . ,#'parse-varint)
           (:timeuuid  . ,#'parse-uuid)
           (:inet      . ,#'parse-ip))))
    (alexandria:alist-hash-table funs)))

(defun parse-option (stream)
  (let* ((id (gethash (read-short stream) +option-id+)))
    (case id
      (:custom
       `(:custom ,(parse-string stream)))
      (:list
       (make-parse-coll (parse-option stream)))
      (:map
       (make-parse-map (juxt (parse-option stream)
                             (parse-option stream))))
      (:set
       (make-parse-coll (parse-option stream)))
      (otherwise
       (gethash id +option-id-to-parser+)))))
