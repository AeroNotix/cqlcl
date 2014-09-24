(in-package :cqlcl)


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
  ((prepare-string :accessor ps :initarg :ps :initform (error "Prepare string required."))))

(defclass execute-header (header)
  ((query-id :accessor qid  :initarg :qid  :initform (error "Prepared Query ID required."))
   (values   :accessor vals :initarg :vals :initform (error "Values required."))))

(defclass prepared-query ()
  ((query-string :accessor qs  :initarg :qs)
   (query-id     :accessor qid :initarg :qid)
   (col-specs    :accessor cs  :initarg :cs)))

(defclass error-response ()
  ((code  :initarg :code  :accessor code)
   (msg   :initarg :msg   :accessor msg)
   (extra :initarg :extra :accessor extra :initform nil)))

(defun parse-schema-change (stream)
  (values t (format nil "~A: ~{~A~^.~}"
                    (parse-string stream)
                    (remove-if #'empty?
                               (list
                                (parse-string stream)
                                (parse-string stream))))))

(defun parse-supported-packet (stream)
  (parse-string-multimap stream))

(defun parse-unavailable-exception (stream)
  (read-short stream)
  (let ((required (read-int stream))
        (alive (read-int stream)))
    (format nil "~d / ~d" required alive)))

(defun parse-write-timeout (stream)
  (read-short stream)
  (let ((received (read-int stream))
        (blockfor (read-int stream))
        (write-type (parse-string stream)))
    (format nil "~a: ~d / ~d" write-type received blockfor)))

(defun parse-read-timeout (stream)
  (read-short stream)
  (let ((received (read-int stream))
        (blockfor (read-int stream))
        (data-present (= (read-byte stream) 1)))
    (format nil "~d / ~d / ~b" received blockfor data-present)))

(defun parse-already-exists (stream)
  (let ((keyspace (parse-string stream))
        (table-name (parse-string stream)))
    (format nil "ALREADY EXISTS: ~a.~a" keyspace table-name)))

(defun parse-error-packet (stream)
  (let* ((error-code (gethash (read-int stream) +error-codes+))
         (error-msg (parse-string stream))
         (error-val (make-instance 'error-response :code error-code :msg error-msg)))
    (setf (extra error-val)
          (case error-code
            (:unavailable-exception
             (parse-unavailable-exception stream))
            (:write-timeout
             (parse-write-timeout stream))
            (:read-timeout
             (parse-read-timeout stream))
            (:already-exists
             (parse-already-exists stream))
            (:unprepared
             (parse-short-bytes stream))))
    error-val))

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
         (qid (make-array size :element-type '(unsigned-byte 8))))
    (assert (= (read-sequence qid stream) size))
    (multiple-value-bind (col-count global-tables-spec) (parse-metadata stream)
      (let ((col-specs (parse-colspecs global-tables-spec col-count stream)))
        (make-instance 'prepared-query :qid qid :cs col-specs)))))

(defun parse-set-keyspace (stream)
  (values t (parse-string stream)))

(defun parse-result-packet (stream)
  (let* ((res-int (read-int stream))
         (res-type (gethash res-int +result-type+)))
    (case res-type
      (:set-keyspace
       (parse-set-keyspace stream))
      (:rows
       (parse-rows stream))
      (:prepared
       (parse-prepared stream))
      (:void
       (values))
      (:schema-change
       (parse-schema-change stream))
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
