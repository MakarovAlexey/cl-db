(in-package #:cl-db)

(defvar *session*)

(defclass clos-session ()
  ((connection :initarg :connection
	       :reader connection-of)
   (mappping-schema :initarg :mapping-schema
		    :reader mapping-schema-of)
   (loaded-objects :initform (make-hash-table)
		   :reader loaded-objects-of)
   (new-objects :initform (list)
		:accessor new-objects-of)
   (removed-objects :initform (list)
		    :accessor removed-objects-of)))

(defun open-session (mapping-schema-fn &rest connection-args)
  (make-instance 'clos-session
		 :mapping-schema (funcall mapping-schema-fn)
		 :connection (apply #'open-database connection-args)))

(defun close-session (session)
  (close-database (connection-of session)))

(defun call-with-session (mapping-schema-fn connection-args thunk)
  (let ((*session*
	 (apply #'open-session mapping-schema-fn connection-args)))
    (funcall thunk)
    (close-session *session*)))

(defmacro with-session ((mapping-schema &rest connection-args)
			&body body)
  `(apply #'call-with-session
	  (function ,mapping-schema)
	  (quote ,connection-args)
	  #'(lambda () ,@body)))

(defun prepare (sql-string &optional (connection (connection-of *session*)))
  (prepare-query connection sql-string sql-string)
  (values sql-string connection))

(defun execute-prepared (name connection &rest parameters)
  (exec-prepared connection name parameters))

;; implement cascade operations
(defun persist-object (object &optional (session *session*))
  (pushnew object (new-objects-of session)))

(defun remove-object (object &optional (session *session*))
  (pushnew object (removed-objects-of session)))
