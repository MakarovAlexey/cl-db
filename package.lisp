;;;; package.lisp

(defpackage #:cl-db
  (:use #:closer-common-lisp)
  (:export :define-database-interface
	   :define-mapping-schema
	   :define-class-mapping
	   :with-session
	   :*default-database-interface*
	   :*default-connection-args*
	   :*default-mapping-schema*
	   :db-persist
	   :db-remove
	   :db-query))
