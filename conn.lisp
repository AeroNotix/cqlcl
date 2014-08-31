(in-package :cqlcl)


(defclass connection ()
  ((streams :accessor streams)
   (version :accessor vsn)))

(defclass cql-stream ()
  ((id :accessor id :initarg id)))
