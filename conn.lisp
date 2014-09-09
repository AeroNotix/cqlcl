(in-package :cqlcl)


(defclass synchronous-connection ()
  ((conn             :accessor conn         :initarg :conn)
   (prepared-queries :accessor pqs          :initform (make-hash-table :test #'equal))
   (conn-options     :accessor conn-options :initarg :options)))

(defun make-connection (&key (connection-type :sync) (host "localhost") (port 9042)); TODO: &key version compression)
  (let* ((c-types '((:sync . synchronous-connection)
                    (:async . connection)))
         (conn (usocket:socket-stream
                (usocket:socket-connect host port :element-type '(unsigned-byte 8))))
         (cxn-type (cdr (assoc connection-type c-types)))
         (cxn (make-instance cxn-type :conn conn)))
    (options cxn)
    (let* ((options (read-single-packet conn)))
      (setf (conn-options cxn) options)
      (startup cxn)
      (assert (eq (read-single-packet conn) :ready))
      cxn)))

(defgeneric options (connection)
  (:documentation "Sends an option request."))

(defgeneric startup (connection &key version compression)
  (:documentation "Sends a startup request."))

(defgeneric prepare (connection statement)
  (:documentation "Prepares a statement."))

(defgeneric query (connection statement)
  (:documentation "Executes a query with no bound values."))

(defgeneric execute (connection statement &rest values)
  (:documentation "Executes a prepared statement with bound values."))

(defmethod startup ((conn synchronous-connection) &key (version "3.0.0") (compression nil))
  (declare (ignore compression)) ;; TODO: Implement compression
  (let* ((options (alexandria:alist-hash-table
                   `(("CQL_VERSION" . ,version))))
         (header (make-instance 'startup-header :op :startup :opts options))
         (cxn (conn conn)))
    (encode-value header cxn)))

(defmethod options ((conn synchronous-connection))
  (let ((header (make-instance 'options-header :op :options))
        (cxn (conn conn)))
    (encode-value header cxn)))

(defmethod prepare ((conn synchronous-connection) (statement string))
  (when (not (gethash statement (pqs conn)))
    (let ((cxn (conn conn))
          (header (make-instance 'prepare-header :op :prepare :ps statement)))
      (encode-value header cxn)
      (let ((prep-results (read-single-packet cxn)))
        (setf (qs prep-results) statement)
        (setf (gethash statement (pqs conn)) prep-results))))
  (values))

(defmethod query ((conn synchronous-connection) (statement string))
  (let ((cxn (conn conn))
        (header (make-instance 'query-header :op :query :qs statement)))
    (encode-value header cxn)
    (read-single-packet cxn)))

(defmethod execute ((conn synchronous-connection) (statement string) &rest values)
  (let* ((cxn (conn conn))
         (qid (gethash statement (pqs conn))))
    (if qid
        (progn
          (encode-value
           (make-instance 'execute-header :op :execute :qid (qid qid) :vals values)
           cxn)
          (read-single-packet cxn))
        (error (format nil "Unprepared query: ~A" statement)))))
