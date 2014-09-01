(in-package :cqlcl)


(defconstant +default-version+ #x01)
(defconstant +header-length+ 8)
(defconstant +packet-type-index+ 3)
(defconstant +request+  #x00)
(defconstant +response+ #x01)

(defconstant +message-types+ (list +request+ +response+))
(defconstant +op-code-name-to-digit+
  (alexandria:alist-hash-table
   '((:error        . #x00)
     (:startup      . #x01)
     (:ready        . #x02)
     (:authenticate . #x03)
     (:credentials  . #x04)
     (:options      . #x05)
     (:supported    . #x06)
     (:query        . #x07)
     (:result       . #x08)
     (:prepare      . #x09)
     (:execute      . #x0a)
     (:register     . #x0b)
     (:event        . #x0c))))
(defconstant +op-code-digit-to-name+
  (rev-hash +op-code-name-to-digit+))
(defconstant +consistency+
  (alexandria:alist-hash-table
   '((:any          . #x00)
     (:one          . #x01)
     (:two          . #x02)
     (:three        . #x03)
     (:quorum       . #x04)
     (:all          . #x05)
     (:local-quorum . #x06)
     (:each-quorum  . #x07))))

(defclass header ()
  ((ptype       :accessor ptype :initarg :ptype :initform +request+)
   (version     :accessor vsn   :initarg :vsn :initform +default-version+)
   (compression :accessor compression :initarg :compression :initform nil)
   (tracing     :accessor tracing :initarg :tracing :initform nil)
   (stream-id   :accessor id    :initarg :id)
   (op-code     :accessor op    :initarg :op)
   (length      :accessor len   :initarg :len :initform 0)
   (body        :accessor body  :initarg :body :initform nil)))

;; TODO: Maybe implement it as a re-entrant parser?
;; (defclass parser ()
;;   ((buf :accessor buf :initform
;;         (make-array 512 :fill-pointer 0 :adjustable t))))

;; (defun make-parser ()
;;   (make-instance 'parser))

(defmethod initialize-instance :after ((header header) &key)
  (when (not (integerp (vsn header)))
    (error (format nil "Version is not valid: ~A" (vsn header))))
  (when (not (integerp (id header)))
    (error (format nil "Stream ID is not valid: ~A" (id header))))
  (when (not (integerp (gethash (op header) +op-code-name-to-digit+)))
    (error (format nil "Unknown op-code: ~A" (op header))))
  (when (not (member (ptype header) +message-types+))
    (error (format nil "Unknown message type: ~A" (ptype header))))
  ;; TODO: This does nothing at the moment. We need to encode the body
  ;; properly.
  (when (body header)
    (setf (body header) (encode-values (body header)))))

(defmacro as-flags (&rest values)
  `(logior
    ,@(mapcar (lambda (value)
                `(ldb (byte 8 0) ,value)) values)))


(defun make-header (opcode body stream-id)
  (let ((h (make-instance 'header :op opcode :id stream-id
                          :tracing nil :compression nil :body body)))
    h))

(defun options (stream &optional (header (make-header :options nil 0)))
  (encode-value header stream))

(defun startup (stream &optional (header (make-header :startup nil 0)))
  (encode-value header stream))

(defun encode-values (values)
  (declare (ignore values))
  (make-in-memory-output-stream))

(defgeneric encode-value (value stream)
  (:documentation "Encodes a value into the CQL wire format."))

(defmethod encode-value ((value header) stream)
  (write-octet (as-flags (ptype value) (vsn value)) stream)
  (write-octet (as-flags (if (compression value) 1 0)
                         (if (tracing value) 1 0)) stream)
  (write-octet (id value) stream)
  (write-octet (gethash (op value) +op-codes+) stream)
  (write-int (len value) stream))

(defmethod encode-value ((value integer) stream)
  (write-int value stream))

(defmethod encode-value ((value string) stream)
  (let ((len (length value)))
    (if (> len 65535)
        (write-int len stream)
        (write-short len stream))
    (write-string value stream)))

(defmethod encode-value ((value hash-table) stream)
  (let ((num-entries (hash-table-count value)))
    (write-short num-entries stream)
    (maphash (lambda (k v)
               (encode-value k stream)
               (encode-value v stream)) value)))

(defmethod encode-value ((value null) stream)
  (write-octet 0 stream))

(defmethod encode-value ((value symbol) stream)
  (write-octet 1 stream))

(defmethod encode-value ((value uuid) stream)
  (write-sequence (uuid-to-byte-array value) stream))

(defmethod encode-value ((value ip) stream)
  (let ((encoded-ip (ip-to-byte-array value)))
    (write-octet (length encoded-ip) stream)
    (write-sequence encoded-ip stream)))

(defun as-string (bv)
  (flexi-streams:octets-to-string bv :external-format :utf-8))

(defun parse-string (stream)
  (let* ((size (read-short stream))
         (buf  (make-array size :fill-pointer 0)))
    (read-sequence buf stream :end size)
    (as-string buf)))

(defun parse-string-list (stream)
  (let* ((size (read-short stream)))
    (loop for i from 1 upto size
       collect
         (parse-string stream))))

(defun parse-supported-packet (packet)
  (let ((packet-stream (make-stream-from-byte-vector packet))
        (options       (make-hash-table)))
    (let ((num-entries (read-short packet-stream)))
      (dotimes (i num-entries)
        (let* ((key (parse-string packet-stream))
               (entries (parse-string-list packet-stream)))
          (setf (gethash key options) entries))))
    options))

(defun parse-packet (packet)
  (case (parse-header (subseq packet 0 +header-length+))
    (:supported
     (parse-supported-packet (subseq packet 8)))))

(defun parse-header (header)
  (let* ((op-code (elt header +packet-type-index+))
         (resp-type (gethash op-code +op-code-digit-to-name+)))
    resp-type))

(defun read-single-packet (conn)
  (let ((out (make-array 512 :fill-pointer 0 :adjustable t)))
    (do ((bite (read-byte conn) (read-byte conn)))
        ((if (not (listen conn))
             (progn
               (vector-push-extend bite out)
               t)
             nil))
      (vector-push-extend bite out))
    out))
