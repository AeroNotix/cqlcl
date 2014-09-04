(in-package :cqlcl)


(defvar +option-id+
  (alexandria:alist-hash-table
   '((#x0000 . :custom)
     (#x0001 . :ascii)
     (#x0002 . :bigint)
     (#x0003 . :blob)
     (#x0004 . :boolean)
     (#x0005 . :counter)
     (#x0006 . :decimal)
     (#x0007 . :double)
     (#x0008 . :float)
     (#x0009 . :int)
     (#x000a . :text)
     (#x000b . :timestamp)
     (#x000c . :uuid)
     (#x000d . :varchar)
     (#x000e . :varint)
     (#x000f . :timeuuid)
     (#x0010 . :inet)
     (#x0020 . :list)
     (#x0021 . :map)
     (#x0022 . :set))
   :test #'equal))

(defun not-implemented (stream)
  (declare (ignore stream))
  (error "Not implemented!"))

(defvar +option-id-to-parser+
  (let ((funs
         (list
          `(:custom    . ,#'not-implemented)
          `(:ascii     . ,#'parse-string)
          `(:bigint    . ,#'parse-bigint)
          `(:blob      . ,#'parse-string)
          `(:boolean   . ,#'parse-boolean)
          `(:counter   . ,#'not-implemented)
          `(:decimal   . ,#'not-implemented)
          `(:double    . ,#'not-implemented)
          `(:float     . ,#'not-implemented)
          `(:int       . ,#'parse-integer)
          `(:text      . ,#'parse-string)
          `(:timestamp . ,#'parse-bigint)
          `(:uuid      . ,#'parse-uuid)
          `(:varchar   . ,#'parse-string)
          `(:varint    . ,#'not-implemented)
          `(:timeuuid  . ,#'parse-uuid)
          `(:inet      . ,#'parse-ip))))
    (alexandria:alist-hash-table funs)))

(defun encode-values (values)
  ;; TODO: Implement this.  It should take a sequence of values which
  ;; then should be MAPCAR'd over to `encode-value' into a stream and
  ;; returned either as a BYTE-VECTOR or a STREAM.
  (declare (ignore values))
  (make-in-memory-output-stream))

(defgeneric encode-value (value stream)
  (:documentation "Encodes a value into the CQL wire format."))

(defmacro as-flags (&rest values)
  `(logior
    ,@(mapcar (lambda (value)
                `(ldb (byte 8 0) ,value)) values)))

(defmethod encode-value ((value header) stream)
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
  (write-octet (as-flags (ptype value) (vsn value)) stream)
  (write-octet (as-flags (if (compression value) 1 0)
                         (if (tracing value) 1 0)) stream)
  (write-octet (id value) stream)
  (write-octet (gethash (op value) +op-code-name-to-digit+) stream)
  (if (body value)
      (let* ((os (flexi-streams:make-in-memory-output-stream))
             (ims (flexi-streams:make-flexi-stream os)))
        (if (member (op value) (list :prepare :query))
            (progn
              (write-int (length (body value)) ims)
              (write-sequence (as-bytes (body value)) ims))
            (encode-value (body value) ims))
        (when (eq (op value) :query) ;; TODO: Make the consistency configurable
          (write-short 1 ims))
        (let ((bv (flexi-streams:get-output-stream-sequence os)))
          (write-int (length bv) stream)
          (write-sequence bv stream)))
      (write-int 0 stream)))

(defun write-length (thing stream)
  (let ((len (length thing)))
    (if (> len 65535)
        (write-int len stream)
        (write-short len stream))
    len))

(defmethod encode-value ((value string) stream)
  (encode-value (as-bytes value) stream))

(defmethod encode-value ((value hash-table) stream)
  (let ((num-entries (hash-table-count value)))
    (write-short num-entries stream)
    (maphash (lambda (k v)
               (encode-value k stream)
               (encode-value v stream)) value)))

(defmethod encode-value ((value null) stream)
  (write-octet 0 stream))

(defmethod encode-value ((value vector) stream)
  (write-length value stream)
  (write-sequence value stream))

(defmethod encode-value ((value symbol) stream)
  (let ((consistency (gethash value +consistency-name-to-digit+)))
    (cond
      ((eq t value)
       (write-octet 1 stream))
      (consistency
       (write-short consistency stream))
      (t
       (error (format nil "Unknown symbol or keyword attempted to be encoded: ~A"
                      value))))))

(defmethod encode-value ((value uuid) stream)
  (write-sequence (uuid-to-byte-array value) stream))

(defmethod encode-value ((value ipv4) stream)
  (let ((encoded-ip (ip-to-integer value)))
    (write-octet 4 stream)
    (write-int encoded-ip stream)))

(defmethod encode-value ((value ipv6) stream)
  (let ((encoded-ip (ip-to-integer value)))
    (write-octet 16 stream)
    (write-ipv6 encoded-ip stream)))

(defun as-string (bv)
  (flexi-streams:octets-to-string bv :external-format :utf-8))

(defun as-bytes (s)
  (flexi-streams:string-to-octets s))

(defun parse-bytes* (stream size &optional (post-process #'identity))
  (print size)
  (let* ((size (max (if (functionp size)
                        (funcall size stream)
                        size) 0))
         (buf  (make-array size :element-type '(unsigned-byte 8))))
    (assert (= (read-sequence buf stream :end size) size))
    (funcall post-process buf)))

(defun parse-boolean (stream &optional size)
  (declare (ignore size))
  (let ((b (read-byte stream)))
    (not (zerop b))))

(defun parse-uuid (stream size)
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

(defun parse-map (stream value-fn)
  (let ((map (make-hash-table :test #'equalp))
        (num-entries (read-short stream)))
    (dotimes (i num-entries)
      (let* ((key (parse-string stream))
             (entry (funcall value-fn stream)))
        (setf (gethash key map) entry)))
    map))

(defun parse-string-multimap (stream)
  (parse-map stream #'parse-string-list))

(defun parse-string-map (stream)
  (parse-map stream #'parse-string))

(defun parse-option-list (stream)
  (loop for x upto (read-short stream)
     collect
       (parse-option stream)))

(defun parse-option (stream)
  (let* ((id (gethash (read-short stream) +option-id+)))
    (case id
      (:custom
       `(:custom ,(parse-string stream)))
      (:list
       `(:list ,(parse-option stream)))
      (:map
       `(:map ,(parse-option stream)
              ,(parse-option stream)))
      (:set
       `(:set ,(parse-option stream)))
      (otherwise
       (gethash id +option-id-to-parser+)))))
