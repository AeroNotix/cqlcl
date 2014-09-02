(defsystem :cqlcl
  :author "Aaron France"
  :version "0.0.1"
  :license "BSD"
  :description "CQLv2 binary protocol"
  :serial t
  :components ((:file "package")
               (:file "util")
               (:file "types")
               (:file "protocol")
               (:file "conn"))
  :depends-on (:pooler
               :alexandria
               :flexi-streams
               :uuid
               :split-sequence
               :usocket)
  :in-order-to ((test-op (test-op :cqlcl-test))))

(defsystem cqlcl-test
  :version "0.0.1"
  :description "CQLv2 binary protocol tests"
  :licence "BSD"
  :components ((:module "test"
                        :components
                        ((:file "tests"))))
  :depends-on (:cqlcl :lisp-unit :alexandria :flexi-streams :uuid)
  :perform (test-op (o s)
                    ;; LISP-UNIT:RUN-ALL-TESTS is a macro, so it can't be called
                    ;; like a function.
                    (eval `(,(intern "RUN-ALL-TESTS" :lisp-unit)
                            :cqlcl-test))))
