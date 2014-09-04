(in-package :cqlcl)


(defvar +default-version+ #x01)
(defvar +header-length+ 8)
(defvar +packet-type-index+ 3)
(defvar +request+  #x00)
(defvar +response+ #x01)

(defvar +message-types+ (list +request+ +response+))
(defvar +op-code-name-to-digit+
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
(defvar +op-code-digit-to-name+
  (rev-hash +op-code-name-to-digit+))
(defvar +consistency-name-to-digit+
  (alexandria:alist-hash-table
   '((:any          . #x00)
     (:one          . #x01)
     (:two          . #x02)
     (:three        . #x03)
     (:quorum       . #x04)
     (:all          . #x05)
     (:local-quorum . #x06)
     (:each-quorum  . #x07))))
(defvar +consistency-digit-to-name+
  (rev-hash +consistency-name-to-digit+))

(define-condition error-response (error)
  ((code :initarg code :reader :text)
   (msg  :initarg msg  :reader :msg)))

(defclass header ()
  ((ptype       :accessor ptype       :initarg :ptype       :initform +request+)
   (version     :accessor vsn         :initarg :vsn         :initform +default-version+)
   (cql-version :accessor cql-vsn     :initarg :cql-vsn     :initform "3.0.0")
   (compression :accessor compression :initarg :compression :initform nil)
   (tracing     :accessor tracing     :initarg :tracing     :initform nil)
   (stream-id   :accessor id          :initarg :id          :initform 0)
   (op-code     :accessor op          :initarg :op          :initform :error)
   (body        :accessor body        :initarg :body        :initform nil)))

(defmethod initialize-instance :after ((header header) &key))

(defun options (stream &key (compression nil) (version "3.0.0"))
  (let ((header (make-instance 'header
                               :op :options
                               :id 0
                               :compression compression
                               :cql-vsn version)))
    (encode-value header stream)
    (force-output stream)))

(defun startup (stream &optional (header (make-instance 'header :op :startup )))
  (let* ((body (alexandria:alist-hash-table
                '(("CQL_VERSION" . "3.0.0")))))
    (setf (body header) body)
    (encode-value header stream)
    (force-output stream)))

(defun prepare (stream string &optional (header (make-instance 'header :op :prepare )))
  (setf (body header) string)
  (encode-value header stream)
  (force-output stream))

(defun query* (stream string &optional (header (make-instance 'header :op :query)))
  (setf (body header) string)
  (encode-value header stream)
  (force-output stream))

(defun parse-header (header)
  (let* ((op-code (elt header +packet-type-index+))
         (resp-type (gethash op-code +op-code-digit-to-name+)))
    resp-type))

(defun parse-supported-packet (packet)
  (let ((packet-stream (make-stream-from-byte-vector packet)))
    (parse-string-multimap packet-stream)))

(defun parse-error-packet (packet)
  (let* ((packet-stream (make-stream-from-byte-vector packet))
         (error-code (read-int packet-stream))
         (error-msg (parse-string packet-stream)))
    (error 'error-response :code error-code :msg error-msg)))

(defun parse-packet (packet)
  (let ((header-type (parse-header (subseq packet 0 +header-length+))))
    (ccase header-type
      (:supported
       (parse-supported-packet (subseq packet 8)))
      (:error
       (parse-error-packet (subseq packet 8)))
      (:ready :ready))))

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
