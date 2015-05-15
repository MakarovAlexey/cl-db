(in-package #:cl-db)

(defvar *session*)

(defvar *flush-states*)

(defclass clos-session ()
  ((connection :initarg :connection
	       :reader connection-of)
   (mappping-schema :initarg :mapping-schema
		    :reader mapping-schema-of)
   (loaded-objects :initform (make-hash-table :test #'equal)
		   :reader loaded-objects-of
		   :documentation
		   "Objects by mapping name and primary key (list* class-name primary-key)")
   (commited-states :initform (make-hash-table :test #'equal)
		    :reader instance-states-of
		    :documentation
		    "Instance commited states by mapping name and object (list class-name object)")
   (new-objects :initform (list)
		:accessor new-objects-of)
   (removed-objects :initform (list)
		    :accessor removed-objects-of)))

(defclass instance-state ()
  ((object :initarg :object
	   :reader object-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (property-values :initarg :property-values
		    :accessor property-values-of)
   (many-to-one-values :initform (list)
		       :accessor many-to-one-values-of
		       :documentation "Loaded many to one values")
   (one-to-many-values :initform (list)
		       :accessor one-to-many-values-of)))

(defclass flush-state (instance-state)
  ((superclass-dependencies :initform (list)
			    :accessor superclass-dependencies-of)
   (many-to-one-dependencies :initform (list)
			     :accessor many-to-one-dependencies-of)
   (one-to-many-appended :initform (list)
			 :accessor appended-to)))

(defclass state-diff (flush-state)
  ((commited-state :initarg :commited-state
		   :reader commited-state-of)
   (removed-from :initform (list)
		 :accessor removed-from)))

(defclass removed-state (state-diff)
  ())

(defclass new-instance (flush-state)
  ())

(defclass commited-state (instance-state)
  ((primary-key :initarg :primary-key
		:reader primary-key-of)
   (column-values :initarg :column-values
		  :reader column-values-of
		  :documentation "alist of values by column names")))

(defun get-object-of (key)
  (second key))

(defun open-session (mapping-schema-fn &rest connection-args)
  (make-instance 'clos-session
		 :mapping-schema (funcall mapping-schema-fn)
		 :connection (apply #'open-database connection-args)))

(defun property-values (object class-mapping)
  (reduce #'(lambda (result property-mapping)
	      (let ((slot-name (slot-name-of property-mapping)))
		(if (slot-boundp object slot-name)
		    (acons property-mapping
			   (slot-value object slot-name)
			   result))))
	  (property-mappings-of class-mapping) :initial-value nil))

;;;; Process only loaded associations. Slot must be bound
(defun compute-removed-references (flush-state uncommited-value
				   one-to-many-mapping)
  (dolist (object (set-difference (value-of one-to-many-value)
				  uncommited-value))
    (push (cons one-to-many-mapping flush-state)
	  (removed-from
	   (ensure-state object (referenced-class-of one-to-many-mapping))))))

(defun compute-existing-references (flush-state uncommited-value
				    one-to-many-mapping)
  (dolist (object uncommited-value)
    (push (cons one-to-many-mapping flush-state)
	  (appended-to
	   (ensure-state object (referenced-class-of one-to-many-mapping))))))

(defun serialized-value (object one-to-many-mapping)
  (apply (getf (rest one-to-many-mapping) :serializer)
	 (slot-value object (slot-name-of one-to-many-mapping))))

(defun compute-diff (object commited-state
		     &optional (flush-states *flush-states*))
  (let* ((class-mapping
	  (class-mapping-of commited-state))
	 (property-values
	  (apply #'property-values object class-mapping))
	 (many-to-one-values
	  (apply #'compute-many-to-one-dependencies
		 object (many-to-one-mappings-of class-mapping)))
	 (flush-state
	  (make-instance 'state-diff
			 :commited-state commited-state
			 :property-values
			 (set-difference property-values
					 (property-values-of commited-state)
					 :test #'equal)
			 :many-to-one-values
			 (set-difference many-to-one-values
					 (many-to-one-values-of commited-state)
					 :test #'equal))))
    (setf (gethash (list (class-name-of class-mapping) object)
		   flush-states)
	  flush-state)
    (dolist (one-to-many-commited-value
	      (one-to-many-values-of commited-state) flush-state)
      (let* ((one-to-many-mapping
	      (mapping-of one-to-many-commited-value))
	     (uncommited-value
	      (serialized-value (object-of flush-state)
				one-to-many-mapping)))
	(compute-removed-references flush-state uncommited-value
				    one-to-many-mapping)
	(compute-existing-references flush-state uncommited-value
				     one-to-many-mapping)))))

(defun get-subclass-mapping (object class-mapping)
  (find-if #'(lambda (class-name)
	       (typep object class-name))
	   (subclass-mappings-of class-mapping)
	   :key #'class-name-of))

(defun insert-superclass-dependencies (object superclass-mappings)
  (reduce #'(lambda (result superclass-mapping)
	      (acons superclass-mapping
		     (insert-state object superclass-mapping)
		     result))
	  superclass-mappings :initial-value nil))

(defun compute-many-to-one-dependencies (object many-to-one-mappings)
  (reduce #'(lambda (result many-to-one-mapping)
	      (let ((slot-name (slot-name-of many-to-one-mapping)))
		(if (slot-boundp object slot-name)
		    (acons many-to-one-flush-state 
			   (ensure-state object (referenced-class-of many-to-one-mapping))
			   result)
		    result)))
	  many-to-one-mappings :initial-value nil))

(defun compute-one-to-many-dependencies (flush-state one-to-many-mappings)
  (dolist (one-to-many-mapping one-to-many-mappings flush-state)
    (compute-existing-references flush-state
				 (serialized-value (object-of flush-state)
						   one-to-many-mapping)
				 one-to-many-mapping)))

(defun insert-state (object class-mapping
		     &optional (flush-states *flush-states*))
  (let ((inserted
	 (make-flush-state 'new-instance
			   :object object
			   :class-mapping class-mapping
			   :property-values
			   (property-values object class-mapping))))
    (setf (gethash (list (class-name-of class-mapping) object)
		   flush-states)
	  inserted)
    (setf (superclass-dependencies-of flush-state)
	  (apply #'insert-superclass-dependencies object
		 (superclass-mappings-of class-mapping)))
    (setf (many-to-one-dependencies-of flush-state)
	  (apply #'compute-many-to-one-dependencies object
		 (many-to-one-mappings-of class-mapping)))
    (apply #'compute-one-to-many-dependencies flush-state
	   (one-to-many-mappings-of class-mapping))
    inserted))

(defun insert-object (object class-mapping)
  (let ((inserted
	 (insert-state object class-mapping))
	(subclass-mapping
	 (get-subclass-mapping object class-mapping)))
    (when (not (null subclass-mapping))
      (insert-object object subclass-mapping))
    inserted))

(defun compute-state (object mapping-name
		      &optional (session *session*))
  (multiple-value-bind (commited-state persistedp)
      (gethash
       (list object mapping-name)
       (instance-states-of session))
    (if (not persistedp)
	(insert-object object mapping-name)
	(compute-diff object commited-state))))

(defun get-flush-state (object mapping-name
			&optional (flush-states *flush-states*))
  (gethash (list object mapping-name) flush-states))

(defun ensure-state (object &optional (mapping-name (type-of object)))
  (let ((state (get-flush-state object mapping-name)))
    (if (null state)
	(compute-state object mapping-name)
	state)))

(defun ensure-inserted (object &optional (class-name (type-of object)))
  (let ((state
	 (get-flush-state object mapping-name)))
    (if (null state)
	(insert-object object (get-class-mapping class-name))
	state)))

(defun ensure-removed (object)
  

;;; cascade removing and setting not removable object's associations
;;; to null (if null value permitted)

(defun begin-transaction (&optional (session *session*))
  (execute "BEGIN" (connection-of session)))

(defun rollback (&optional (session *session*))
  (execute "ROLLBACK" (connection-of session)))

(defun commit (&optional (session *session*))
  (execute "COMMIT" (connection-of session)))

(defun flush-session (session)
  (let ((*flush-states* (make-hash-table :test #'equal)))
    (dolist (object (new-objects-of session))
      (ensure-inserted object))
    (dolist (object (removed-objects-of session))
      (ensure-removed object))
    (dolist (commit-state (alexandria:hash-table-keys
			   (instance-states-of session)))
      (ensure-diff commit-state))))

(defun close-session (session)
  (flush-session session)
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
  #'(lambda (&rest parameters)
      (exec-prepared sql-string connection parameters)))

(defun execute-prepared (name connection &rest parameters)
  (exec-prepared connection name parameters))

(defun db-persist (object &optional (session *session*))
  (pushnew
   (list object (class-name (class-of object)))
   (new-objects-of session)))

(defun db-remove (object &optional (session *session*))
  (pushnew object (removed-objects-of session)))

(defun get-object (class-name primary-key
		   &optional (session *session*))
  (gethash
   (list* class-name primary-key)
   (loaded-objects-of session)))

(defun register-object (object class-name primary-key
			&optional (session *session*))
  (setf (gethash
	 (list* class-name primary-key)
	 (loaded-objects-of session))
	object))

(defun ensure-commited-state (object &optional (session *session*))
  (ensure-gethash object
		  (instance-states-of session)
		  (make-instance 'commited :object object)))
