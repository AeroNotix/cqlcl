(in-package :cqlcl)


(defclass connection ()
  ((streams :accessor streams)
   (version :accessor vsn)))

