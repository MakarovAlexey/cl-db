(in-package #:cl-db)

(defvar *session*)

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
   (many-to-one :initarg :many-to-one
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
		(acons property-mapping
		       (when (slot-boundp object slot-name)
			 (slot-value object slot-name))
		       result)))
	  (property-mappings-of class-mapping)
	  :initial-value nil))

(defun serialize-value (value one-to-many-mapping)
  (apply (getf (rest one-to-many-mapping) :serializer) value))

(defun compute-one-to-many-dependencies (flush-states flush-state
					 &optional one-to-many-mapping
					 &rest one-to-many-mappings)
  (if (not (null one-to-many-mapping))
      (let ((object
	     (object-of flush-state))
	    (flush-states
	     (apply #'compute-one-to-many-dependencies
		    flush-states flush-state one-to-many-mappings))
	    (slot-name (slot-name-of one-to-many-mapping)))
	(if (slot-boundp object slot-name)
	    (reduce #'(lambda (flush-states object)
			(multiple-value-bind (flush-states one-to-many-flush-state)
			    (ensure-state flush-states object
					  (referenced-class-of one-to-many-mapping))
			  (setf (appended-to one-to-many-flush-state)
				(acons (foreign-key-of one-to-many-mapping)
				       flush-state
				       (appended-to flush-state)))
			  flush-states))
		    (serialize-value (slot-value object slot-name)
				     one-to-many-mapping)
		    :initial-value flush-states)
	    flush-states))
      flush-states))
  
(defun compute-many-to-one-dependencies (flush-states object
					 &optional many-to-one-mapping
					 &rest many-to-one-mappings)
  (if (not (null many-to-one-mapping))
      (let ((slot-name (slot-name-of many-to-one-mapping)))
	(when (slot-boundp object slot-name)
	  (multiple-value-bind (flush-states many-to-one-dependencies)
	      (apply #'compute-many-to-one-dependencies
		     flush-states object many-to-one-mappings)
	    (multiple-value-bind (flush-states many-to-one-flush-state)
		(ensure-state flush-states object
			      (referenced-class-of many-to-one-mapping))
	      (values flush-states
		      (acons many-to-one-flush-state
			     many-to-one-mapping
			     many-to-one-dependencies))))))
      (values flush-states nil)))

(defun insert-state (flush-states object class-mapping)
  (let* ((property-values
	  (property-values object class-mapping))
	 (flush-state
	  (make-instance 'new-instance
			 :object object
			 :class-mapping class-mapping
			 :property-values property-values)))
    (multiple-value-bind (flush-states superclass-dependencies)
	(apply #'insert-superclass-dependencies
	       (list* flush-state flush-states) object
	       (superclass-mappings-of class-mapping))
      (setf (superclass-dependencies-of flush-state)
	    superclass-dependencies)
      (multiple-value-bind (flush-states many-to-one-dependencies)
	  (apply #'compute-many-to-one-dependencies flush-states
		 object (many-to-one-mappings-of class-mapping))
	(setf (many-to-one-dependencies-of flush-state)
	      many-to-one-dependencies)
	(values (apply #'compute-one-to-many-dependencies
		       flush-states flush-state
		       (one-to-many-mappings-of class-mapping))
		flush-state)))))

(defun insert-superclass-dependencies (flush-states object
				       &optional superclass-mapping
				       &rest superclass-mappings)
  (if (not (null superclass-mapping))
      (multiple-value-bind (flush-states superclass-dependencies)
	  (apply #'insert-superclass-dependencies
		 flush-states object superclass-mappings)
	(multiple-value-bind (flush-states flush-state)
	    (insert-state flush-states object superclass-mapping)
	  (values flush-states
		  (acons superclass-mapping flush-state
			 superclass-dependencies))))
      (values flush-states nil)))

(defun insert-subclass-states (flush-states object class-mapping)
  (let ((subclass-mapping
	 (find-if #'(lambda (class-name)
		      (typep object class-name))
		  (subclass-mappings-of class-mapping)
		  :key #'class-name-of)))
    (if (not (null subclass-mapping))
	(insert-object flush-states object subclass-mapping)
	flush-states)))

(defun insert-object (flush-states object class-mapping)
  (multiple-value-bind (flush-states flush-state)
      (insert-state flush-states object class-mapping)
    (values (insert-subclass-states flush-states flush-state
				    class-mapping)
	    flush-state)))

(defun compute-one-to-many-removed-objects (flush-states state-diff
					    one-to-many-mapping)
  (let ((object (object-of state-diff))
	(slot-name (slot-name-of one-to-many-mapping)))
    (when (slot-boundp object slot-name)
      (reduce #'(lambda (flush-states object)
		  (multiple-value-bind (flush-states flush-state)
		      (ensure-state flush-states object
				    (referenced-class-of one-to-many-mapping))
		    (setf (removed-from flush-state)
			  (acons one-to-many-mapping state-diff
				 (removed-from flush-state)))
		    flush-states))
	      (set-difference
	       (slot-value object slot-name)
	       (rest (assoc one-to-many-mapping
			    (one-to-many-values-of state-diff))))
	      :initial-value flush-states))))
    
(defun compute-state-diff-dependants (flush-states state-diff
				      one-to-many-mappings)
  (reduce #'(lambda (flush-states one-to-many-mapping)
	      (compute-one-to-many-dependencies
	       (compute-one-to-many-removed-objects flush-states
						    state-diff
						    one-to-many-mapping)
	       state-diff one-to-many-mapping))
	  one-to-many-mappings
	  :initial-value flush-states))

(defun compute-diff (flush-states object commited-state)
    (let* ((class-mapping
	    (class-mapping-of commited-state))
	   (property-values
	    (apply #'property-values object class-mapping))
	   (many-to-one-values
	    (apply #'many-to-one-values object class-mapping))
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
					   :test #'equal)))
	   (one-to-many-mappings
	    (one-to-many-mappings-of class-mapping)))
      (compute-state-diff-dependants flush-states flush-state
				     one-to-many-mappings)))

(defun get-flush-state (flush-states object mapping-name)
  (find-if #'(lambda (flush-state)
	       (and
		(eq (object-of flush-state) object)
		(eq (mapping-name-of
		     (mapping-of flush-state)) mapping-name)))
	   flush-states))

(defun compute-state (flush-states object mapping-name
		      &optional (session *session*))
  (multiple-value-bind (commited-state persistedp)
      (gethash object (instance-states-of session))
    (if (not persistedp)
	(insert-object flush-states object mapping-name)
	(compute-diff flush-states object commited-state))))

(defun ensure-state (flush-states object &optional (mapping-name (type-of object)))
  (let ((state (get-flush-state flush-states object mapping-name)))
    (if (not (null state))
	(values flush-states state)
	(compute-state flush-states object mapping-name))))

(defun ensure-inserted-state (flush-states object
			      &optional (mapping-name (type-of object)))
  (let ((state
	 (get-flush-state flush-states object mapping-name)))
    (if (not (null state))
	(values flush-states state)
	(insert-object flush-states object mapping-name))))

(defun insert-objects (&optional (session *session*))
  (reduce #'ensure-inserted-state
	  (new-objects-of session)
	  :initial-value nil))

(defun update-objects (flush-states &optional (session *session*))
  (reduce #'(lambda (flush-states object-and-mapping-name)
	      (apply #'ensure-state
		     flush-states object-and-mapping-name))
	  (alexandria:hash-table-keys
	   (instance-states-of session))
	  :initial-value flush-states))

(defun remove-objects (flush-states session)
  (dolist (object (removed-objects-of session) flush-states)
    (dolist (flush-state (remove object flush-states :test-not #'eq))
      (change-class flush-state 'removed-state))))

;;; cascade removing and setting not removable object's associations
;;; to null (if null value permitted)

(defun begin-transaction (&optional (session *session*))
  (execute "BEGIN" (connection-of session)))

(defun rollback (&optional (session *session*))
  (execute "ROLLBACK" (connection-of session)))

(defun commit (&optional (session *session*))
  (execute "COMMIT" (connection-of session)))

(defun flush-session (session)
  (update-objects
   (remove-objects
    (insert-objects session) session) session))

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
