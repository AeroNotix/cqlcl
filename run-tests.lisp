(in-package #:cl-user)

#+sbcl
(require :sb-cover)

(defmacro with-silence (&body body)
  `(let ((*standard-output* (make-broadcast-stream))
         (*error-output* (make-broadcast-stream))
         (*trace-output* (make-broadcast-stream)))
     ,@body))

#+sbcl
(declaim (optimize sb-cover:store-coverage-data))

(with-silence
    (progn
      ;; The tests generate table names, so we set the random state.
      (setf *random-state* (make-random-state t))
      (ql:quickload :fiveam)))

(asdf:test-system :cqlcl)

#+sbcl
(progn
  (sb-cover:report "./coverage/")
  (declaim (optimize (sb-cover:store-coverage-data 0))))
