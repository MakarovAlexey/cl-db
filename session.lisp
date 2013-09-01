(in-package #:cl-db)

(defvar *default-database-interface*)
(defvar *default-connection-args*)
(defvar *default-mapping-schema*)

(defmacro with-session ((&rest options) &body body)
  `(call-with-session
    (quote ,@(compile-option :database-interface options))
    (quote ,@(compile-option :mapping-schema options))
    (list ,@(compile-option :connection-args options))
    #'(lambda () ,@body)))

(defclass mapping-configuration ()
  ((mapping-schema :initarg :mapping-schema
		   :reader mapping-schema-of)
   (open-connection :initarg :open-connection-function
		    :reader open-connection-function-of)
   (close-connection :initarg :close-connection-function
		     :reader close-connection-function-of)
   (prepare-statement :initarg :prepare-statement-function
		      :reader prepare-statement-function-of)
   (execute-statement :initarg :execute-statement-function
		      :reader execute-statement-function-of)
   (list-metadata :initarg :list-metadata-function
		  :reader list-metadata-function-of)))

(defun open-connection (configuration &rest args)
  (apply (open-connection-function-of configuration) args))

(defun close-connection (configuration)
  (funcall (close-connection-function-of configuration)))

(defclass clos-session ()
  ((mappings :initarg :mappings
	     :reader mappings-of)
   (connection :initarg :connection
	       :reader connection-of)
   (loaded-objects :initarg :loaded-objects
		   :reader loaded-objects-of)
   (prepared-statement-counter :initform 0
			       :accessor prepared-statement-counter-of)))

(defun prepare-statement (session query)
  (funcall
   (prepare-statement-function-of
    (configuration-of session))
   (connection-of session)
   (format nil "prepaired_statement_~a"
	   (incf (prepared-statement-counter-of session)))
   query))

(defvar *session*)

(defun call-with-session (session function)
  (let ((*session* session))
    (funcall function)))


