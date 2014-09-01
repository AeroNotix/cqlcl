(in-package :cqlcl)


(defun write-sized (value n stream)
  (let ((value (ldb (byte n 0) value)))
    (loop for i from (/ n 8) downto 1
       do
         (write-byte (ldb (byte 8 (* (1- i) 8)) value) stream))))

(defun read-sized (n stream)
  (apply #'+
         (loop for i from (/ n 8) downto 1
            collect
              (dpb (read-byte stream) (byte 8 (* (1- i) 8)) 0))))

(defun create-symbol (fmt name)
  (intern (format nil fmt name) 'cqlcl))

(defmacro define-binary-write (name size)
  (let ((value (gensym "value"))
        (stream (gensym "stream"))
        (write-fun-name (create-symbol "WRITE-~A" name))
        (read-fun-name (create-symbol "READ-~A" name)))
    `(progn
       (defun ,write-fun-name (,value ,stream)
         (write-sized ,value ,size ,stream))
       (defun ,read-fun-name (,stream)
         (read-sized ,size ,stream)))))

(defun make-in-memory-output-stream ()
  (flexi-streams:make-flexi-stream
   (flexi-streams:make-in-memory-output-stream)))

(define-binary-write octet  8)
(define-binary-write short 16)
(define-binary-write int   32)
