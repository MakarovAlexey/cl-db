;;;; package.lisp

(defpackage #:cl-db
  (:use #:closer-common-lisp #:alexandria)
  (:export :db-persist
	   :db-read
	   :db-remove
	   :with-session
	   :with-transaction))
