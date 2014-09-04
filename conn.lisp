(in-package :cqlcl)


(defclass synchronous-connection ()
  ;; TODO maybe this should hold an instance of a parser
  ((conn :accessor conn :initarg :conn)
   (conn-options :accessor conn-options :initarg :options)))

(defun make-connection (&key (synchronous t) (host "localhost") (port 9042)); TODO: &key version compression)
  (let ((conn (usocket:socket-stream
               (usocket:socket-connect host port :element-type '(unsigned-byte 8)))))
    (options conn)
    (let* ((options (parse-packet (read-single-packet conn)))
           (cxn-type (if synchronous 'synchronous-connection 'connection))
           (cxn (make-instance cxn-type :conn conn :options options)))
      (startup conn)
      (assert (eq (parse-packet (read-single-packet conn)) :ready))
      cxn)))

(defgeneric prepare-statement (connection statement)
  (:documentation "Prepares a statement."))

(defgeneric query (connection statement)
  (:documentation "Executes a query."))

(defmethod prepare-statement ((conn synchronous-connection) (statement string))
  (let ((cxn (conn conn)))
    (prepare cxn statement)
    (read-single-packet cxn)))

(defmethod query ((conn synchronous-connection) (statement string))
  (let* ((cxn (conn conn)))
    (query* cxn statement)
    (read-single-packet cxn)))
