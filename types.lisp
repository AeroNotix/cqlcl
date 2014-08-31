(in-package :cqlcl)


(defun write-sized (value n stream)
  (let ((value (ldb (byte n 0) value)))
    (loop for i from (/ n 8) downto 1
         do
         (write-byte (ldb (byte n (* (1- i) 8)) value) stream))))
