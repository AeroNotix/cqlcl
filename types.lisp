(in-package :cqlcl)


(defun write-sized (value n stream)
  (let ((value (ldb (byte n 0) value)))
    (loop for i from (/ n 8) downto 1
         do
         (write-byte (ldb (byte n (* (1- i) 8)) value) stream))))

(defmacro define-binary-write (name size)
  (let ((arg1 (gensym "value"))
        (arg2 (gensym "stream"))
        (fun-name (intern (format nil "WRITE-~A" name) 'cqlcl)))
    `(defun ,fun-name (,arg1 ,arg2)
       (write-sized ,arg1 ,size ,arg2))))

(define-binary-write octet  8)
(define-binary-write short 16)
(define-binary-write int   32)
