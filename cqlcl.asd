(defsystem :cqlcl
  :author "Aaron France"
  :version "0.0.1"
  :license "BSD"
  :description "CQLv2 binary protocol"
  :serial t
  :components ((:file "package")
               (:file "util")
               (:file "constants")
               (:file "types")
               (:file "headers")
               (:file "protocol")
               (:file "conn"))
  :depends-on (:pooler
               :alexandria
               :flexi-streams
               :uuid
               :split-sequence
               :bordeaux-threads
               :usocket)
  :defsystem-depends-on (:fiveam)
  :in-order-to ((test-op (test-op :cqlcl-test))))

(defsystem :cqlcl-test
  :version "0.0.1"
  :description "CQLv2 binary protocol tests"
  :licence "BSD"
  :components ((:module "test"
                        :components
                        ((:file "tests"))))
  :depends-on (:cqlcl :fiveam :alexandria :flexi-streams :uuid)
  :perform (test-op :after (o s)
                    (fiveam:run! :cqlcl)))
