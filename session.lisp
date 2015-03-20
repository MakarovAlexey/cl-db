(in-package #:cl-db)

(defvar *session*)

(defclass clos-session ()
  ((connection :initarg :connection
	       :reader connection-of)
   (mappping-schema :initarg :mapping-schema
		    :reader mapping-schema-of)
   (loaded-objects :initform (make-hash-table :test #'equal)
		   :reader loaded-objects-of)
   (instance-states :initarg :instance-states
		    :reader instance-states-of)
   (new-objects :initform (list)
		:accessor new-objects-of)
   (removed-objects :initform (list)
		    :accessor removed-objects-of)))

(defclass instance-state ()
  ((object :initarg :object
	   :reader object-of)
   (mapping :initarg :mapping
	    :reader mapping-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (properties :initarg :properties
	       :reader property-values-of)
   (many-to-one :initarg :many-to-one
		:reader many-to-one-values-of)
   (one-to-many :initarg :one-to-many
		:reader one-to-many-values-of)
   (inverted-one-to-many :initform (list)
			 :accessor inverted-one-to-many-of
			 :documentation
			 "Many-to-one side of other one-to-many relations")))

(defclass commited-state (instance-state)
  ())

(defclass new-instance (instance-state)
  ())

(defclass dirty-instance (instance-state)
  ())

(defclass removed-instance (instance-state)
  ())

(defun slot-name-of (slot-mapping)
  (first slot-mapping))

(defun referenced-class-of (slot-mapping)
  (getf (rest slot-mapping) :referenced-class-name))

(defun get-object-of (key)
  (second key))

(defun open-session (mapping-schema-fn &rest connection-args)
  (make-instance 'clos-session
		 :mapping-schema (funcall mapping-schema-fn)
		 :connection (apply #'open-database connection-args)))

(defun property-values (object &optional property &rest properties)
  (when (not (null property))
    (let ((slot-name (slot-name-of property)))
      (when (slot-boundp object slot-name)
	(acons (slot-value object slot-name)
	       property
	       (apply #'property-values properties))))))

(defun many-to-one-values (object &optional many-to-one-mapping
			   &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (let ((slot-name (slot-name-of many-to-one-mapping)))
      (when (slot-boundp object slot-name)
	(acons (slot-value object slot-name)
	       many-to-one-mapping
	       (apply #'many-to-one-values
		      object many-to-one-mappings))))))

(defun serialize-value (value one-to-many-mapping)
  (apply (getf (rest one-to-many-mapping) :serializer) value))

(defun one-to-many-values (object &optional one-to-many-mapping
			   &rest one-to-many-mappings)
  (when (not (null one-to-many-mapping))
    (let ((slot-name (slot-name-of one-to-many-mapping)))
      (when (slot-boundp object slot-name)
	(acons (serialize-value (slot-value object slot-name)
				one-to-many-mapping)
	       one-to-many-mapping
	       (apply #'one-to-many-values
		      object one-to-many-mappings))))))

(defun superclasses-object-values (object &optional superclass-mapping
				   &rest superclasses-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (superclasses-property-values
			  superclasses-many-to-one-values
			  superclasses-one-to-many-values)
	(apply #'superclasses-object-values
	       object superclasses-mappings)
      (multiple-value-bind (property-values
			    many-to-one-values one-to-many-values)
	  (apply #'object-values object superclass-mapping)
	(values
	 (append property-values
		 superclasses-property-values)
	 (append many-to-one-values
		 superclasses-many-to-one-values)
	 (append one-to-many-values
		 superclasses-one-to-many-values))))))

(defun object-values (object &key properties one-to-many-mappings
			       many-to-one-mappings
			       superclass-mappings &allow-other-keys)
  (multiple-value-bind (property-values
			many-to-one-values
			one-to-many-values)
      (apply #'superclasses-object-values
	     object superclass-mappings)
    (values
     (append (apply #'property-values
		    object properties)
	     property-values)
     (append (apply #'many-to-one-values
		    object many-to-one-mappings)
	     many-to-one-values)
     (append (apply #'one-to-many-values
		    object one-to-many-mappings)
	     one-to-many-values))))

(defun insert-object (object class-mapping)
  (multiple-value-bind (property-values
			many-to-one-values one-to-many-values)
      (apply #'object-values object class-mapping)
    (make-instance 'new-instance
		   :object object
		   :property-values property-values
		   :many-to-one-values many-to-one-values
		   :one-to-many-values one-to-many-values)))

;; check set value to unbound slot-value
(defun compute-dirty (object commited-state)
  (multiple-value-bind (property-values
			many-to-one-values one-to-many-values)
      (apply #'object-values object class-mapping)
    (make-instace 'dirty
		  :properties
		  (set-difference property-values
				  (property-values-of commited-state)
				  :test #'equal)
		  :many-to-one-values
		  (set-difference many-to-one-values
				  (many-to-one-values-of commited-state)
				  :test #'equal)
		  :one-to-many-values
		  (set-difference one-to-many-values
				  (one-to-many-values-of commited-state)
				  :test #'equal))))

(defun invert-one-to-many (dirty-state object referenced-objects
			   one-to-many-mapping &rest one-to-many-values)
  (multiple-value-bind (state dirty-state)
      (ensure-state dirty-state referenced-object)
    (setf (inverted-one-to-many-of state)
	  (reduce #'(lambda (result referenced-object)
		      (acons referenced-object
			     one-to-many-mapping result))
		  referenced-objects
		  :initial-value (inverted-one-to-many-of state)))
    dirty-state))

(defun compute-state (dirty-state object
		      &optional (session *session*))
  (multiple-value-bind (commited-state persistedp)
      (gethash object (instance-states-of session))
    (let ((state
	   (if (not persistedp)
	       (insert-object object
			      (get-class-mapping
			       (class-name
				(class-of object)) 
			       (mapping-schema-of session)))
	       (compute-dirty (object-of commited-state)
			      commited-state))))
      (values state
	      (invert-one-to-many
	       (list* state dirty-state)
	       (one-to-many-values-of state))))))

(defun ensure-state (dirty-state object)
  (or
   (values (find object dirty-state :key #'object-of)
	   dirty-state)
   (compute-state dirty-state object)))

(defun insert-objects (session dirty-states
		       &optional (session *session*))
  (reduce #'(lambda (result object)
	      (ensure-state dirty-state object)
	  (new-objects-of session)
	  :initial-value nil))

(defun begin-transaction (session)
  (execute "BEGIN" (connection-of session)))

(defun rollback (session)
  (execute "ROLLBACK" (connection-of session)))

(defun commit (transaction)
  (execute "COMMIT" (connection-of session)))

(defun flush-session (session)
  (remove-objects session
		  (update-objects session
				  (insert-objects session))))

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
  (pushnew object (new-objects-of session)))

(defun db-remove (object &optional (session *session*))
  (pushnew object (removed-objects-of session)))

(defun get-object (class-name primary-key
		   &optional (session *session*))
  (gethash
   (list* class-name primary-key)
   (loaded-objects-of *session*)))

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
