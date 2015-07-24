;;;; cl-db.asd

(asdf:defsystem #:cl-db
  :serial t
  :description "common lisp object-relational mapping system"
  :author "Makarov Alexey <alexeys@@yandex.ru>"
  :license "Specify license here"
  :serial t
  :depends-on (#:alexandria
	       #:closer-mop
	       #:cl-postgres)
  :components ((:file "package")
;;	       (:file "database-interface")
	       (:file "cl-db")
;;	       (:file "mapping-schema")
	       (:file "session")
	       (:file "persistence")
;;	       (:file "expressions")
	       (:file "writers")
	       (:file "query-classes")
	       (:file "query-plan")
	       ;;(:file "query")
	       ;;(:file "db-read")))
))
(asdf:defsystem #:cl-db.tests
  :serial t
  :description "Describe cl-db here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:cl-db
	       #:lift)
  :components ((:file "tests")))
