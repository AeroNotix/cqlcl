(in-package :cqlcl)


(defvar +default-version+ #x01)
(defvar +header-length+ 8)
(defvar +packet-type-index+ 3)
(defvar +request+  #x00)
(defvar +response+ #x01)
(defvar +global-tables-spec+ (ldb (byte 16 0) #x0001))
(defvar +has-more-pages+ (ldb (byte 16 0) #x0002))
(defvar +no-meta-data+ (ldb (byte 16 0) #x0004))
(defvar +message-types+ (list +request+ +response+))
(defvar +result-type+
  (alexandria:alist-hash-table
   '((#x01 . :void)
     (#x02 . :rows)
     (#x03 . :set-keyspace)
     (#x04 . :prepared)
     (#x05 . :shema-change))
   :test #'equal))
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

(defun parse-supported-packet (stream)
  (parse-string-multimap stream))

(defun parse-error-packet (stream)
  (let* ((error-code (read-int stream))
         (error-msg (parse-string stream)))
    (print error-msg)
    (error 'error-response :code error-code :msg error-msg)))

(defun row-flag-set? (flags flag)
  (gethash flag
           (alexandria:alist-hash-table
            `((:global-tables-spec . ,(plusp (logand flags +global-tables-spec+)))
              (:has-more-tables    . ,(plusp (logand flags +has-more-pages+)))
              (:no-meta-data       . ,(plusp (logand flags +no-meta-data+))))
            :test #'equal)))

(defun parse-colspec (name-prefixes? stream)
  (let ((name (parse-string stream)))
    (when name-prefixes?
      (parse-string stream)
      (parse-string stream))
    (list name (parse-option stream))))

(defun parse-row (col-specs stream)
  (let ((row nil))
    (loop for (col-name parser) in col-specs
       do (let ((size (parse-int stream)))
            (push (when (plusp size)
                    (funcall parser stream size)) row)))
    (reverse row)))

(defun parse-rows* (col-specs stream)
  (let ((num-rows (read-int stream)))
    (when (not (zerop num-rows))
      (loop for i from 0 upto (1- num-rows)
         collect
           (parse-row col-specs stream)))))

(defun parse-rows (stream)
  (multiple-value-bind (col-count global-tables-spec) (parse-metadata stream)
    (let ((col-specs (parse-colspecs global-tables-spec col-count stream)))
      (parse-rows* col-specs stream))))

(defun parse-prepared (stream)
  (let* ((size (parse-short stream))
         (qid (read-sized (* size 8) stream)))
    (multiple-value-bind (col-count global-tables-spec) (parse-metadata stream)
      (let ((col-specs (parse-colspecs global-tables-spec col-count stream)))
        (list qid col-specs)))))

(defun parse-result-packet (stream)
  (let* ((res-int (read-int stream))
         (res-type (gethash res-int +result-type+)))
    (case res-type
      (:set-keyspace t)
      (:rows
       (parse-rows stream))
      (:prepared
       (parse-prepared stream))
      (otherwise stream))))

(defun parse-metadata (stream)
  (let* ((flags (read-int stream))
         (col-count (read-int stream))
         (global-tables-spec (when (row-flag-set? flags :global-tables-spec)
                               (list (parse-string stream)
                                     (parse-string stream)))))
    (values col-count global-tables-spec flags)))

(defun parse-colspecs (global-tables-spec col-count stream)
  (loop for i upto (1- col-count)
     collect
       (parse-colspec (not global-tables-spec) stream)))

(defun parse-header (header)
  (let* ((op-code (elt header +packet-type-index+))
         (resp-type (gethash op-code +op-code-digit-to-name+)))
    resp-type))

(defun read-single-packet (conn)
  (let* ((header-type (parse-header (parse-bytes conn 8))))
    (ccase header-type
      (:supported
       (parse-supported-packet conn))
      (:error
       (parse-error-packet conn))
      (:ready :ready)
      (:result
       (parse-result-packet conn)))))
