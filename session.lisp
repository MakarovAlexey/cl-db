(in-package #:cl-db)

(defvar *default-database-interface-name*)

(defvar *default-connection-args*)

(defvar *default-mapping-schema-name*)

(defvar *session*)

(defclass clos-session ()
  ((mapping-schema :initarg :mapping-schema
		   :reader mapping-schema-of)
   (database-interface :initarg :database-interface
		       :reader database-interface-of)
   (database-connection :initarg :database-connection
			:reader database-connection-of)
   (loaded-objects :initform (make-hash-table)
		   :reader loaded-objects-of)
   (new-objects :initform (make-hash-table)
		:accessor new-objects-of)
   (removed-objects :initform (make-hash-table)
		    :accessor removed-objects-of)))

(defun open-session (mapping-schema
		     database-interface &rest connection-args)
  (make-instance 'clos-session
		 :mapping-schema mapping-schema
		 :database-interface database-interface
		 :database-connection (apply #'open-connection
					     database-interface
					     connection-args)))

(defun close-session (session)
  (close-connection (database-interface-of session)
		    (database-connection-of session)))

(defun call-with-session (function database-interface-name
			  mapping-schema-name &rest connection-args)
  (let* ((*session*
	  (apply #'open-session
		 (ensure-mapping-schema mapping-schema-name)
		 (get-database-interface database-interface-name)
		 connection-args)))
    (funcall function)
    (close-session *session*)))

(defmacro with-session ((&rest options) &body body)
  `(apply #'call-with-session
	  #'(lambda () ,@body)
	  (quote ,(or (first
		       (compile-option :database-interface options))
		      *default-database-interface*))
	  (quote ,(or (first
		       (compile-option :mapping-schema options))
		      *default-mapping-schema*))
	  (list ,@(or (compile-option :connection-args options)
		      *default-connection-args*))))

;; 1. надо проверять открытие и закрытие соединения