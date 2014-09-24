(in-package :cqlcl)


(defclass connection ()
  ((conn :accessor conn :initarg :conn)
   (prepared-queries :accessor pqs :initform (make-hash-table :test #'equal))
   (conn-options :accessor conn-options :initarg :options)))

(defclass async-connection (connection)
  ((used-streams :accessor used-streams :initform nil)
   (pending-queries :accessor pending-queries :initform (make-hash-table :test #'equal))
   (mutex :accessor mutex :initform (make-lock))))

(defun make-connection (&key (connection-type :sync) (host "localhost") (port 9042)); TODO: &key version compression)
  (let* ((c-types '((:sync . connection)
                    (:async . async-connection)))
         (conn (usocket:socket-stream
                (usocket:socket-connect host port :element-type '(unsigned-byte 8))))
         (cxn-type (cdr (assoc connection-type c-types)))
         (cxn (make-instance cxn-type :conn conn)))
    (let* ((options (options cxn)))
      (setf (conn-options cxn) options)
      (assert (eq (startup cxn) :ready))
      cxn)))

(defgeneric read-single-packet (connection)
  (:documentation "Reads a single CQL reply packet from a CQL connection."))

(defmethod read-single-packet ((conn connection))
  (let ((cxn (conn conn)))
    (let* ((header-type (parse-header (parse-bytes cxn 8))))
      (ccase header-type
        (:supported
         (parse-supported-packet cxn))
        (:error
         (parse-error-packet cxn))
        (:ready :ready)
        (:result
         (parse-result-packet cxn))))))

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

(defmethod options ((conn connection))
  (let ((header (make-instance 'options-header :op :options))
        (cxn (conn conn)))
    (encode-value header cxn)
    (read-single-packet conn)))

(defmethod startup ((conn connection) &key (version "3.0.0") (compression nil))
  (declare (ignore compression)) ;; TODO: Implement compression
  (let* ((options (alexandria:alist-hash-table
                   `(("CQL_VERSION" . ,version))))
         (header (make-instance 'startup-header :op :startup :opts options))
         (cxn (conn conn)))
    (encode-value header cxn)
    (read-single-packet conn)))

(defmethod prepare ((conn connection) (statement string))
  (when (not (gethash statement (pqs conn)))
    (let ((cxn (conn conn))
          (header (make-instance 'prepare-header :op :prepare :ps statement)))
      (encode-value header cxn)
      (let ((prep-results (read-single-packet conn)))
        (setf (qs prep-results) statement)
        (setf (gethash statement (pqs conn)) prep-results))))
  (values))

(defmethod query ((conn connection) (statement string))
  (let ((cxn (conn conn))
        (header (make-instance 'query-header :op :query :qs statement)))
    (encode-value header cxn)
    (read-single-packet conn)))

(defmethod execute ((conn connection) (statement string) &rest values)
  (let* ((cxn (conn conn))
         (qid (gethash statement (pqs conn))))
    (if qid
        (progn
          (encode-value
           (make-instance 'execute-header :op :execute :qid (qid qid) :vals values)
           cxn)
          (read-single-packet conn))
        (error (format nil "Unprepared query: ~A" statement)))))

(defun next-stream-id* (used-streams)
  (when (= (length used-streams) 254)
    (error "Error because no streamz left.")) ;; TODO MAKE THIS A PROPER CONDITION
  (if (empty? used-streams)
      1
      (let ((stream-id (loop for i from 1 upto 255
                          do
                            (when (not (equal (nth (1- i) used-streams) i))
                              (return i)))))
        stream-id)))

(defgeneric next-stream-id (connection)
  (:documentation "Returns the next available stream id for a connection."))

(defgeneric return-stream-id (connection id)
  (:documentation "Returns a stream id to the pool of ids."))

(defmethod next-stream-id ((conn async-connection))
  (with-lock-held ((mutex conn))
    (let ((stream-id (next-stream-id* (used-streams conn))))
      (setf (used-streams conn) (sort (cons stream-id (used-streams conn)) #'<))
      stream-id)))

(defmethod return-stream-id ((conn async-connection) (i integer))
  (with-lock-held ((mutex conn))
    (when (not (member i (used-streams conn)))
      (error "ID not in use")) ;; TODO: Make this better
    (setf (used-streams conn) (remove i (used-streams conn))))
  (values))

(defmacro with-next-stream-id (var aconn &body body)
  (let ((connsym (gensym)))
    `(let* ((,connsym ,aconn)
            (,var (next-stream-id ,connsym)))
       (unwind-protect
            ,@body
         (return-stream-id ,connsym ,var)))))
