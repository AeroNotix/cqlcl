(in-package :cqlcl)


(defun write-sized (value n stream)
  (let ((value (ldb (byte n 0) value)))
    (loop for i from (/ n 8) downto 1
       do
         (write-byte (ldb (byte 8 (* (1- i) 8)) value) stream))))

(defun read-sized (n stream &optional (signed? nil))
  (let ((unsigned-value 0)
        (byte-size (/ n 8)))
    (dotimes (i byte-size)
      (setf unsigned-value (+ (* unsigned-value #x100)
                              (read-byte stream))))
    (if signed?
        (if (>= unsigned-value (ash 1 (1- (* 8 byte-size))))
            (- unsigned-value (ash 1 (* 8 byte-size)))
            unsigned-value)
        unsigned-value)))

(defmacro define-binary-write (name size)
  (labels ((create-symbol (fmt name)
             (intern (format nil fmt name) 'cqlcl)))
    (let ((value (gensym "value"))
          (stream (gensym "stream"))
          (write-fun-name (create-symbol "WRITE-~A" name))
          (read-fun-name (create-symbol "READ-~A" name)))
      `(progn
         (defun ,write-fun-name (,value ,stream)
           (write-sized ,value ,size ,stream))
         (defun ,read-fun-name (,stream &key (signed? nil))
           (read-sized ,size ,stream signed?))))))

(defun make-in-memory-output-stream ()
  (flexi-streams:make-flexi-stream
   (flexi-streams:make-in-memory-output-stream)))

(define-binary-write octet  8)
(define-binary-write short 16)
(define-binary-write int   32)

(defclass ip () ())

(defclass ipv4 (ip)
  ((addr :accessor addr :initarg :addr :initform "0.0.0.0")))

(defclass ipv6 (ip)
  ((addr :accessor addr :initarg :addr :initform "0:0:0:0:0:0:0:0")))

(defgeneric ip-to-byte-array (ip)
  (:documentation "Returns a byte array representing an IP Address."))

(defun parse-ip (ip delimiter byte-spec &optional (radix 10))
  (map 'vector (lambda (octet)
                 (ldb byte-spec (parse-integer octet :radix radix)))
       (split-sequence delimiter (addr ip))))

(defmethod ip-to-byte-array ((ip ipv4))
  (parse-ip ip #\. (byte 8 0)))

(defmethod ip-to-byte-array ((ip ipv6))
  (parse-ip ip #\: (byte 16 0) 16))
