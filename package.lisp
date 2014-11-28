;;;; package.lisp

(defpackage #:cl-db
  (:use #:closer-common-lisp #:alexandria)
  (:export :define-database-interface
	   :define-mapping-schema
	   :define-class-mapping
	   :with-session
	   :*default-database-interface-name*
	   :*default-mapping-schema-name*
	   :*default-connection-args*
	   :db-persist
	   :db-remove
	   :db-query))
