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
               :split-sequence))
