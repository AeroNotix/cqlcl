(in-package :cqlcl)


(define-condition error-response (error)
  ((code :initarg code :reader :text)
   (msg  :initarg msg  :reader :msg)))

(defclass header ()
  ((ptype       :accessor ptype       :initarg :ptype       :initform +request+)
   (version     :accessor vsn         :initarg :vsn         :initform +default-version+)
   (cql-version :accessor cql-vsn     :initarg :cql-vsn     :initform "3.0.0")
   (compression :accessor compression :initarg :compression :initform nil)
   (consistency :accessor consistency :initarg :consistency :initform :quorum)
   (tracing     :accessor tracing     :initarg :tracing     :initform nil)
   (stream-id   :accessor id          :initarg :id          :initform 0)
   (op-code     :accessor op          :initarg :op          :initform :error)
   (body        :accessor body        :initarg :body        :initform nil)))

(defclass options-header (header)
  ())

(defclass startup-header (header)
  ((options :accessor opts :initarg :opts)))

(defclass query-header (header)
  ((query-string :accessor qs :initarg :qs :initform (error "Query string required."))))

(defclass prepare-header (header)
  ((prepare-string :accessor ps :initarg :ps :initform (error "Query string required."))))

(defun options (conn)
  (let ((header (make-instance 'options-header :op :options))
        (cxn (conn conn)))
    (encode-value header cxn)))

(defun startup (conn &key (version "3.0.0") (compression nil))
  (declare (ignore compression)) ;; TODO: Implement compression
  (let* ((options (alexandria:alist-hash-table
                   `(("CQL_VERSION" . ,version))))
         (header (make-instance 'startup-header :op :startup :opts options))
         (cxn (conn conn)))
    (encode-value header cxn)))

(defun prepare (conn string)
  (let ((cxn (conn conn))
        (header (make-instance 'prepare-header :op :prepare :ps string)))
  (encode-value header cxn)))

(defun query* (stream string values)
  (let ((header (make-instance 'query-header :op :query :qs string)))
    (encode-value header stream)))

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
