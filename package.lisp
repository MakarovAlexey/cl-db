;;;; package.lisp

(defpackage #:cl-db
  (:use #:closer-common-lisp #:alexandria)
  (:import-from :cl-postgres
		:open-database
		:close-database
		:connection-meta
		:prepare-query
		:exec-prepared
		:exec-query)
  (:export :db-persist
	   :db-read
	   :db-remove
	   :with-session
	   :with-transaction))
