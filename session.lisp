(in-package #:cl-db)

(defvar *session*)

(defvar *transaction*)

(defclass clos-session ()
  ((connection :initarg :connection
	       :reader connection-of)
   (mappping-schema :initarg :mapping-schema
		    :reader mapping-schema-of)
   (loaded-objects :initform (make-hash-table :test #'equal)
		   :reader loaded-objects-of)))

(defclass clos-transaction ()
  ((session :initarg :session
	    :reader session-of)
   (object-snapshots :initarg :snapshots
		     :reader object-snapshots-of)
   (new-objects :initform (list)
		:accessor new-objects-of)
   (removed-objects :initform (list)
		    :accessor removed-objects-of)))

(defun get-object (class-name primary-key
		   &optional (objects
			      (loaded-objects-of *session*)))
  (gethash (list* class-name primary-key)
	   objects))

(defun register-object (object class-name primary-key
			&optional (objects
				   (loaded-objects-of *session*)))
  (setf (gethash (list* class-name primary-key)
		 objects)
	object))

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

(defun execute (sql-string connection)
  (exec-query connection sql-string))

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

(defun begin-transaction (session)
  (execute "BEGIN" (connection-of session))
  (make-instance 'clos-transaction
		 :snapshots (make-snapshot
			     (loaded-objects-of session)
			     (mapping-schema-of session))))

(defun rollback (transaction)
  (execute "ROLLBACK" (connection-of
		       (session-of transaction))))

(defun commit (transaction)
  (execute "COMMIT" (connection-of
		     (session-of transaction))))

(defun call-with-transaction (session thunk)
  (let ((*transaction* (begin-transaction session)))
    (funcall thunk)
    (commit *transaction*)))

(defmacro with-transaction ((&key (session *session*)) &body body)
  `(apply #'call-with-transaction
	  session #'(lambda () ,@body)))
