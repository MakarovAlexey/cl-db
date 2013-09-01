;;; database-interface.lisp

(in-package #:cl-db)

(defvar *database-interfaces* (make-hash-table))

(defclass database-interface ()
  ((open-connection-fn :initarg :open-connection-fn
		       :reader open-connection-fn-of)
   (close-conection-fn :initarg :close-conection-fn
		       :reader close-conection-fn-of)
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
		       :close-conection-fn close-conection-fn
		       :prepare-statement-fn prepare-statement-fn
		       :execute-statement-fn execute-statement-fn)))
   
(defmacro define-database-interface (name &body options)   
  `(ensure-database-interface
    (quote ,name)
    ,@(apply #'compile-option :open-connection options)
    ,@(apply #'compile-option :close-connection options)
    ,@(apply #'compile-option :prepare options)
    ,@(apply #'compile-option :execute options)))

;;(defun use-interface ())

;;(defun open-connection ())

;;(defun close-connection ())

;;(defun begin-transaction ())

;;(defun commit-transaction ())

;;(defun rollback-transaction ())

;;(defun prepare ())

;;(defun execute ())