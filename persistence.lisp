(in-package #:cl-db)

(defvar *transaction*)

(defclass clos-transaction ()
  ((session :initarg :session
	    :reader session-of)
   (object-snapshots :initarg :snapshots
		     :reader object-snapshots-of)
   (new-objects :initform (list)
		:accessor new-objects-of)
   (removed-objects :initform (list)
		    :accessor removed-objects-of)))

;; implement cascade operations
(defun persist-object (object &optional (session *session*))
  (pushnew object (new-objects-of session)))

(defun remove-object (object &optional (session *session*))
  (pushnew object (removed-objects-of session)))

(defun make-snapshot (primary-key object class-mapping)

(defun make-snapshots (objects mapping-schema)
  (let ((table (make-hash-table :size (length objects))))
    (maphash #'(lambda (pk-and-class-name object)
		 (setf (gethash object table)
		       (make-snapshot (rest pk-and-class-name)
				      object
				      (get-class-mapping
				       (class-name
					(class-of object))))))
	     objects)
    table))

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
	  ,session #'(lambda () ,@body)))
