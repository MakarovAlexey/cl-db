;;; database-interface.lisp

(in-package #:cl-db)

(defvar *database-interfaces* (make-hash-table))

(defun get-database-interface (name)
  (multiple-value-bind (database-interface presentp)
      (gethash name *database-interfaces*)
    (when (not presentp)
      (error "Database interface ~a not found" name))
    database-interface))

(defclass database-interface ()
  ((open-connection-fn :initarg :open-connection-fn
		       :reader open-connection-fn-of)
   (close-connection-fn :initarg :close-connection-fn
		       :reader close-connection-fn-of)
   (prepare-statement-fn :initarg :prepare-statement-fn 
			 :reader prepare-statement-fn-of)
   (execute-statement-fn :initarg :execute-statement-fn
		      :reader execute-statement-fn-of)))

(defun ensure-database-interface (name open-connection-fn
				  close-conection-fn
				  prepare-statement-fn
				  execute-statement-fn)
  (setf (gethash name *database-interfaces*)
	(make-instance 'database-interface
		       :open-connection-fn open-connection-fn
		       :close-connection-fn close-conection-fn
		       :prepare-statement-fn prepare-statement-fn
		       :execute-statement-fn execute-statement-fn)))
   
(defmacro define-database-interface (name &body options)   
  `(ensure-database-interface
    (quote ,name)
    ,@(compile-option :open-connection options)
    ,@(compile-option :close-connection options)
    ,@(compile-option :prepare options)
    ,@(compile-option :execute options)))

(defun open-connection (database-interface &rest args)
  (apply (open-connection-fn-of database-interface) args))

(defun close-connection (database-interface database-connection)
  (funcall (close-connection-fn-of database-interface)
	   database-connection))

(defun prepare-statement (session query)
  (funcall
   (prepare-statement-function-of
    (configuration-of session))
   (connection-of session)
   (format nil "prepaired_statement_~a"
	   (incf (prepared-statement-counter-of session)))
   query))

;;(defun begin-transaction ())

;;(defun commit-transaction ())

;;(defun rollback-transaction ())

;;(defun execute ())