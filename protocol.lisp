(in-package :cqlcl)


(defconstant +default-version+ #x01)
(defconstant +request+  #x00)
(defconstant +response+ #x01)
(defconstant +message-types+ (list +request+ +response+))
(defconstant +op-codes+
  (alexandria:alist-hash-table
   '((:error        . #x00)
     (:startup      . #x01)
     (:ready        . #x02)
     (:authenticate . #x03)
     (:credentials  . #x04)
     (:options      . #x05)
     (:supported    . #x06)
     (:query        . #x07)
     (:result       . #x08)
     (:prepare      . #x09)
     (:execute      . #x0a)
     (:register     . #x0b)
     (:event        . #x0c))))
(defconstant +consistency+
  (alexandria:alist-hash-table
   '((:any          . #x00)
     (:one          . #x01)
     (:two          . #x02)
     (:three        . #x03)
     (:quorum       . #x04)
     (:all          . #x05)
     (:local-quorum . #x06)
     (:each-quorum  . #x07))))

(defclass header ()
  ((ptype       :accessor ptype :initarg :ptype)
   (version     :accessor vsn   :initarg :vsn :initform +default-version+)
   (compression :accessor compression :initarg :compression :initform nil)
   (stream-id   :accessor id    :initarg :id)
   (op-code     :accessor op    :initarg :op)
   (length      :accessor len   :initarg :len)))

(defgeneric write-to (stream value))

(defmethod initialize-instance :after ((header header) &key)
  (when (not (integerp (vsn header)))
    (error (format nil "Version is not valid: ~A" (vsn header))))
  (when (not (integerp (id header)))
    (error (format nil "Stream ID is not valid: ~A" (id header))))
  (when (not (integerp (gethash (op header) +op-codes+)))
    (error (format nil "Unknown op-code: ~A" (op header))))
  (when (not (member (ptype header) +message-types+))
    (error (format nil "Unknown message type: ~A" (ptype header))))
  (setf (len header) (file-position (body header))))

(defgeneric encode-value (value stream)
  (:documentation "Encodes a value into the CQL wire format."))

(defmethod encode-value ((value header) stream)
  (write-octet (logior
                (ldb (byte 8 0) (ptype value))
                (ldb (byte 8 0) (vsn value))) stream)
  (write-octet (if (compression value) 1 0) stream)
  (write-octet (id value) stream)
  (write-octet (gethash (op value) +op-codes+) stream)
  (write-int (len value) stream))

(defmethod encode-value ((value integer) stream)
  (write-int value stream))
