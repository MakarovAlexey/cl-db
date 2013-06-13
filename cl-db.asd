;;;; cl-db.asd

(asdf:defsystem #:cl-db
  :serial t
  :description "common lisp object-relational mapping system"
  :author "Makarov Alexey <alexeys@@yandex.ru>"
  :license "Specify license here"
  :depends-on (#:alexandria
	       #:postmodern
	       #:closer-mop)
  :components ((:file "package")
               (:file "cl-db")))

(asdf:defsystem #:cl-db.tests
  :serial t
  :description "Describe cl-db here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:cl-db
	       #:lift)
  :components ((:file "tests")))