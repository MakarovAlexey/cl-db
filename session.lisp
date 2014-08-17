(in-package #:cl-db)

(defvar *default-database-interface-name*)

(defvar *default-connection-args*)

(defvar *session*)

(defclass clos-session ()
  ((database-interface :initarg :database-interface
		       :reader database-interface-of)
   (database-connection :initarg :database-connection
			:reader database-connection-of)
   (loaded-objects :initform (make-hash-table)
		   :reader loaded-objects-of)
   (new-objects :initform (list)
		:accessor new-objects-of)
   (removed-objects :initform (list)
		    :accessor removed-objects-of)))

(defun open-session (database-interface &rest connection-args)
  (make-instance 'clos-session
		 :database-interface database-interface
		 :database-connection (apply #'open-connection
					     database-interface
					     connection-args)))

(defun close-session (session)
  (close-connection (database-interface-of session)
		    (database-connection-of session)))

(defun call-with-session (function database-interface-name
			  &rest connection-args)
  (let* ((*session*
	  (apply #'open-session
		 (get-database-interface database-interface-name)
		 connection-args)))
    (funcall function)
    (close-session *session*)))

(defmacro with-session ((&rest options) &body body)
  `(apply #'call-with-session
	  #'(lambda () ,@body)
	  (quote ,(or (first
		       (compile-option :database-interface options))
		      *default-database-interface-name*))
	  (list ,@(or (compile-option :connection-args options)
		      *default-connection-args*))))

;; implement cascade operations
(defun persist-object (object &optional (session *session*))
  (pushnew object (new-objects-of session)))

(defun remove-object (object &optional (session *session*))
  (pushnew object (removed-objects-of session)))
