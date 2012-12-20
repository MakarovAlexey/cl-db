;;;; cl-db.asd

(asdf:defsystem #:cl-db
  :serial t
  :description "Describe cl-db here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:closer-mop)
  :components ((:file "package")
               (:file "cl-db")))

