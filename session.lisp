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
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (properties :initarg :properties
	       :accessor property-values-of)
   (many-to-one :initarg :many-to-one
		:accessor many-to-one-values-of
		:documentation "Loaded many to one values")
   (one-to-many-values :initform (list)
		       :accessor one-to-many-values-of)))

(defclass flush-state (instance-state)
  ((one-to-many-appended :initarg :one-to-many-appended
			 :reader appended-to)
   (inverted-one-to-many-appended :initform (list)
				  :accessor inverted-appendings)))

(defclass state-diff (flush-state)
  ((commited-state :initarg :commited-state
		   :reader commited-state-of)
   (one-to-many-removed :initarg :one-to-many-removed
			:reader removed-from)
   (inverted-one-to-many-removed :initform (list)
				 :accessor inverted-removings)))

(defclass new-instance (flush-state)
  ())

(defclass commited-state (instance-state)
  ((primary-key :initarg :primary-key
		:reader primary-key-of)
   (many-to-one-keys :initform (list)
		     :accessor many-to-one-keys-of
		     :documentation "Many to one keys")
   (inverted-one-to-many-keys :initform (list)
			      :accessor inverted-one-to-many-keys-of
			      :documentation
			      "Many-to-one side of other one-to-many relations")))

(defun (setf many-to-one-value) (value commited-state many-to-one-mapping)
  (setf
   (many-to-one-values-of commited-state)
   (acons many-to-one-mapping value
	  (many-to-one-values-of commited-state)))
  (setf (slot-value
	 (object-of commited-state)
	 (slot-name-of many-to-one-mapping))
	value))

(defun (setf one-to-many-value) (value commited-state one-to-many-mapping)
  (setf
   (one-to-many-values-of commited-state)
   (acons one-to-many-mapping value
	  (one-to-many-values-of commited-state)))
  (setf (slot-value
	 (object-of commited-state)
	 (slot-name-of one-to-many-mapping))
	value))

(defun (setf property-value) (value commited-state property-mapping)
  (setf
   (property-values-of commited-state)
   (acons property-mapping value
	  (property-values-of commited-state)))
  (setf (slot-value
	 (object-of commited-state)
	 (slot-name-of property-mapping))
	value))

(defun (setf many-to-one-key) (value commited-state many-to-one-mapping)
  (setf
   (many-to-one-keys-of commited-state)
   (acons many-to-one-mapping value
	  (many-to-one-keys-of commited-state))))

(defun slot-name-of (slot-mapping)
  (first slot-mapping))

(defun value-of (value-mapping)
  (first value-mapping))

(defun mapping-of (value-mapping)
  (rest value-mapping))

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
	(reduce #'(lambda (result object)
		    (acons object one-to-many-mapping result))
		(serialize-value (slot-value object slot-name)
				 one-to-many-mapping)
		:initial-value (apply #'one-to-many-values
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
		   :mapping class-mapping
		   :property-values property-values
		   :many-to-one-values many-to-one-values
		   :one-to-many-appended-values
		   (reduce #'append one-to-many-values
			   :key #'value-of :initial-value nil))))

;; check set value to unbound slot-value
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

(defun compute-state (dirty-state object
		      &optional (session *session*))
  (multiple-value-bind (commited-state persistedp)
      (gethash object (instance-states-of session))
    (let* ((class-mapping
	    (get-class-mapping
	     (class-name
	      (class-of object)) 
	     (mapping-schema-of session)))
	   (state
	    (if (not persistedp)
		(insert-object object class-mapping)
		(compute-diff (object-of commited-state)
			      class-mapping
			      commited-state))))
      ;; добавить обработку many-to-one
      (values (invert-one-to-many dirty-state state)
	      state))))

(defun ensure-state (dirty-state object)
  (let ((state (find object dirty-state :key #'object-of)))
    (if (not (null state))
	(compute-state dirty-state object)
	(values dirty-state state))))

(defun insert-objects (&optional (session *session*))
  (reduce #'ensure-state
	  (new-objects-of session)
	  :initial-value nil))

(defun update-objects (dirty-state session)
  (reduce #'ensure-state
	  (alexandria:hash-table-keys
	   (instance-states-of session))
	  :initial-value dirty-state))

(defun begin-transaction (&optional (session *session*))
  (execute "BEGIN" (connection-of session)))

(defun rollback (&optional (session *session*))
  (execute "ROLLBACK" (connection-of session)))

(defun commit (&optional (session *session*))
  (execute "COMMIT" (connection-of session)))

(defun flush-session (session)
  (remove-objects (update-objects (insert-objects session)
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
  (pushnew object (new-objects-of session)))

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
