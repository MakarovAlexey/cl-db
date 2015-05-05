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
   (properties :initarg :properties
	       :accessor property-values-of)
   (many-to-one :initarg :many-to-one
		:accessor many-to-one-values-of
		:documentation "Loaded many to one values")
   (one-to-many-values :initform (list)
		       :accessor one-to-many-values-of)))

(defclass dependency ()
  ((flush-state :initarg :flush-state
		:reader flush-state-of)))

(defclass superclass-dependency (dependency)
  ((superclass-mapping :initarg :superclass-mapping
		       :reader superclass-mapping-of)))

(defclass many-to-one-dependency (dependency)
  ((many-to-one-mapping :initarg :many-to-one-mapping
			:reader many-to-one-mapping-of)))

(defclass inverted-one-to-many-dependency (dependency)
  ((one-to-many-mapping :initarg :one-to-many-mapping
			:reader one-to-many-mapping-of)))

(defclass flush-state (instance-state)
  ((one-to-many-appended :initarg :one-to-many-appended
			 :reader appended-to)
   (inverted-one-to-many-appended :initform (list)
				  :accessor appended-to)))

(defclass state-diff (flush-state)
  ((commited-state :initarg :commited-state
		   :reader commited-state-of)
   (inverted-one-to-many-removed :initform (list)
				 :accessor inverted-removings)))

(defclass new-instance (flush-state)
  ())

(defclass commited-state (instance-state)
  ((primary-key :initarg :primary-key
		:reader primary-key-of)
   (column-values :initarg :column-values
		  :reader column-values-of
		  :documetation "alist of values by column names")))

(defun get-object-of (key)
  (second key))

(defun open-session (mapping-schema-fn &rest connection-args)
  (make-instance 'clos-session
		 :mapping-schema (funcall mapping-schema-fn)
		 :connection (apply #'open-database connection-args)))

(defun property-values (object &optional property &rest properties)
  (when (not (null property))
    (let ((slot-name (slot-name-of property-mapping)))
      (acons (when (slot-boundp object slot-name)
	       (slot-value object slot-name))
	     property-mapping
	     (apply #'property-values properties)))))

(defun serialize-value (value one-to-many-mapping)
  (apply (getf (rest one-to-many-mapping) :serializer) value))

(defun compute-one-to-many-dependencies (flush-states flush-state
					 &optional one-to-many-mapping
					 &rest one-to-many-mappings)
  (when (not (null one-to-many-mapping))
    (let ((flush-states
	   (apply #'compute-one-to-many-states
		  flush-states flush-state one-to-many-mappings))
	  (slot-name (slot-name-of one-to-many-mapping)))
      (if (slot-boundp object slot-name)
	  (reduce #'(lambda (flush-states object)
		      (multiple-value-bind (flush-states one-to-many-flush-state)
			  (ensure-state flush-states object
					(referenced-class-name-of one-to-many-mapping))
			(setf (appended-to one-to-many-flush-state)
			      (acons (foreign-key-of one-to-many-mapping)
				     flush-state
				     (appended-to flush-state)))
			flush-states))
		  (serialize-value (slot-value object slot-name)
				   one-to-many-mapping)
		  :initial-value flush-states)
	  flush-states))))
  
(defun compute-many-to-one-dependencies (flush-states object
					 &optional many-to-one-mapping
					 &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (let ((slot-name (slot-name-of many-to-one-mapping)))
      (when (slot-boundp object slot-name)
	(multiple-value-bind (flush-states many-to-one-dependencies)
	    (apply #'many-to-one-values
		   flush-states object many-to-one-mappings)
	  (multiple-value-bind (flush-states many-to-one-flush-state)
	      (ensure-state flush-states object
			    (referenced-class-name-of many-to-one-mapping))
	    (values flush-states
		    (acons many-to-one-flush-state
			   many-to-one-mapping
			   many-to-one-dependencies))))))))

(defun insert-state (flush-states object class-mapping)
  (let* ((property-values
	  (apply #'property-values object class-mapping))
	 (flush-state
	  (make-instance 'new-instance
			 :object object
			 :mapping class-mapping
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
  (when (not (null superclass-mapping))
    (multiple-value-bind (flush-states superclass-dependencies)
	(apply #'insert-superclass-dependencies
	       flush-states object superclass-mappings)
      (multiple-value-bind (flush-states flush-state)
	  (insert-state flush-states object superclass-mapping)
	(values flush-states
		(acons superclass-mapping flush-state
		       superclass-dependencies))))))

(defun insert-subclass-states (flush-states object class-mapping)
  (let ((subclass-mapping
	 (find-if #'(lambda (class-name)
		      (typep object class-name))
		  (subclass-mappings-of class-mapping)
		  :key class-name-of)))
    (if (not (null subclass-mapping))
	(insert-object flush-states object subclass-mapping)
	flush-states)))

(defun insert-object (flush-states object class-mapping)
  (multiple-value-bind (flush-states flush-state)
      (insert-state flush-states object class-mapping)
    (values (insert-subclass-states flush-state class-mapping)
	    flush-state)))

(defgeneric invert-one-to-many (dirty-state state))

(defun invert-appended (dirty-state state)
  (reduce #'(lambda (dirty-state one-to-many-reference)
	      (multiple-value-bind (state dirty-state)
		  (ensure-state dirty-state
				(value-of one-to-many-reference))
		(setf (inverted-appendings state)
		      (acons
		       (mapping-of one-to-many-reference)
		       (object-of state)
		       (inverted-appendings state)))
		dirty-state))
	  (appended-to state)
	  :initial-value dirty-state))

(defmethod invert-one-to-many (dirty-state (state flush-state))
  (invert-appended dirty-state state))

(defun invert-removed (dirty-state state)
  (reduce #'(lambda (dirty-state one-to-many-reference)
	      (multiple-value-bind (state dirty-state)
		  (ensure-state dirty-state
				(value-of one-to-many-reference))
		(setf (inverted-removings state)
		      (acons
		       (mapping-of one-to-many-reference)
		       (object-of state)
		       (inverted-removings state)))
		dirty-state))
	  (removed-from state)
	  :initial-value dirty-state))

(defmethod invert-one-to-many (dirty-state (state state-diff))
  (invert-removed (invert-appended dirty-state state) state))

;;; check set value to unbound slot-value
(defun compute-diff (object class-mapping commited-state)
  (multiple-value-bind (property-values
			many-to-one-values one-to-many-values)
      (apply #'object-values object class-mapping)
    (make-instance 'state-diff
		   :commited-state commited-state
		   :mapping class-mapping
		   :property-values
		   (set-difference property-values
				   (property-values-of commited-state)
				   :test #'equal)
		   :many-to-one-values
		   (set-difference many-to-one-values
				   (many-to-one-values-of commited-state)
				   :test #'equal)
		   :one-to-many-appended-values
		   (reduce #'append
			   one-to-many-values
			   :initial-value nil
			   :key #'(lambda (one-to-many-value)
				    (set-difference
				     (value-of one-to-many-value)
				     (rest
				      (assoc
				       (mapping-of one-to-many-value)
				       (one-to-many-values commited-state)))
				     :test #'equal)))
		   :one-to-many-removed-values
		   (reduce #'append
			   one-to-many-values
			   :initial-value nil
			   :key #'(lambda (one-to-many-value)
				    (set-difference
				     (rest
				      (assoc
				       (mapping-of one-to-many-value)
				       (one-to-many-values commited-state)))
				     (value-of one-to-many-value)))))))

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
	(compute-diff flush-states commited-state))))

(defun ensure-state (flush-states object mapping-name)
  (let ((state (get-flush-state flush-states object mapping-name)))
    (if (not (null state))
	(compute-state dirty-state object)
	flush-states)))

(defun insert-objects (dirty-state session)
  (reduce #'(lambda (flush-states object-and-mapping-name)
	      (apply #'ensure-state flush-states
		     object-and-mapping-name))
	  (new-objects-of session)
	  :initial-value dirty-state))

(defun update-objects (&optional (session *session*))
  (reduce #'(lambda (flush-states object-and-mapping-name)
	      (apply #'ensure-state
		     flush-states object-and-mapping-name))
	  (alexandria:hash-table-keys
	   (instance-states-of session))
	  :initial-value nil))

(defun begin-transaction (&optional (session *session*))
  (execute "BEGIN" (connection-of session)))

(defun rollback (&optional (session *session*))
  (execute "ROLLBACK" (connection-of session)))

(defun commit (&optional (session *session*))
  (execute "COMMIT" (connection-of session)))

(defun flush-session (session)
  (remove-objects (insert-objects (update-objects session)
				  session)
		  session))

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
   (list object (class-name-of object))
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
