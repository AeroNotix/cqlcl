(in-package :cqlcl)


(defclass connection ()
  ;; TODO maybe this should hold an instance of a parser
  ((conn :accessor conn :initarg :conn)
   (conn-options :accessor conn-options :initarg :options)))

(defun make-connection (&optional (host "localhost") (port 9042)); TODO: &key version compression)
  (let ((conn (usocket:socket-stream
               (usocket:socket-connect host port :element-type '(unsigned-byte 8)))))
    (options conn)
    (force-output conn)
    (let ((options (parse-packet (read-single-packet conn))))
      (make-instance 'connection :conn conn :options options))))
