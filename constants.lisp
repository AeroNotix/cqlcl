(in-package :cqlcl)


(defvar +default-version+ #x01)
(defvar +header-length+ 8)
(defvar +packet-type-index+ 3)
(defvar +request+  #x00)
(defvar +response+ #x01)
(defvar +global-tables-spec+ (ldb (byte 16 0) #x0001))
(defvar +has-more-pages+ (ldb (byte 16 0) #x0002))
(defvar +no-meta-data+ (ldb (byte 16 0) #x0004))
(defvar +message-types+ (list +request+ +response+))
(defvar +result-type+
  (alexandria:alist-hash-table
   '((#x01 . :void)
     (#x02 . :rows)
     (#x03 . :set-keyspace)
     (#x04 . :prepared)
     (#x05 . :shema-change))
   :test #'equal))
(defvar +op-code-name-to-digit+
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
(defvar +op-code-digit-to-name+
  (rev-hash +op-code-name-to-digit+))
(defvar +consistency-name-to-digit+
  (alexandria:alist-hash-table
   '((:any          . #x00)
     (:one          . #x01)
     (:two          . #x02)
     (:three        . #x03)
     (:quorum       . #x04)
     (:all          . #x05)
     (:local-quorum . #x06)
     (:each-quorum  . #x07))))
(defvar +consistency-digit-to-name+
  (rev-hash +consistency-name-to-digit+))
