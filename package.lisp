;;;; package.lisp

(defpackage #:cl-db
  (:use #:closer-common-lisp)
  (:export :*default-configuration*
	   :define-mapping-schema
	   :use-mapping-schema
	   :with-session
	   :db-persist
	   :db-remove
	   :db-query))
