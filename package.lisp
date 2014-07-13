;;;; package.lisp

(defpackage #:cl-db
  (:import-from #:lisa
		#:defrule
		#:assert-instance
		#:with-inference-engine
		#:reset
		#:run)
  (:use #:closer-common-lisp)
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
