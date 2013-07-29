(in-package #:cl-db)

(defvar *mapping-configurations* (make-hash-table))

(defvar *default-configuration*)

(defun register-mapping-configuration (configuration default)
  (setf (gethash (name-of configuration)
		 *mapping-configurations*) configuration)
  (when default
    (setf *default-configuration* configuration)))

(defun get-mapping-configuration (name)
  (gethash name *mapping-configurations*))

(defclass mapping-configuration ()
  ((name :initarg :name
	 :reader name-of)
   (mapping-schema :initarg :mapping-schema
		   :reader mapping-schema-of)
   (open-connection :initarg :open-connection-function
		    :reader open-connection-function-of)
   (close-connection :initarg :close-connection-function
		     :reader close-connection-function-of)
   (prepare-statement :initarg :prepare-statement-function
		      :reader prepare-statement-function-of)
   (execute-statement :initarg :execute-statement-function
		      :reader execute-statement-function-of)
   (list-metadata :initarg :list-metadata-function
		  :reader list-metadata-function-of)))

(defmacro define-configuration ((name mapping-name)
				(&rest params) &body body)
  `(register-mapping-configuration
    (make-instance 'mapping-configuration :name (quote ,name)
		   :mapping-schema (compile-mapping ,mapping-name)
		   :open-connection-function ,@(apply #'compile-option
						      :open-connection body)
		   :close-connection-function ,@(apply #'compile-option
						       :close-connection body)
		   :prepare-statement-function ,@(apply #'compile-option
							:prepare body)
		   :execute-statement-function ,@(apply #'compile-option
							:execute body)
		   :list-metadata-function ,@(apply #'compile-option
						    :list-metadata body))
    ,(apply #'compile-option :default params))
   `(quote ,name))

(defun open-connection (configuration &rest args)
  (apply (open-connection-function-of configuration) args))

(defun close-connection (configuration)
  (funcall (close-connection-function-of configuration)))

(defclass clos-session ()
  ((mappings :initarg :mappings
	     :reader mappings-of)
   (connection :initarg :connection
	       :reader connection-of)
   (loaded-objects :initarg :loaded-objects
		   :reader loaded-objects-of)
   (prepared-statement-counter :initform 0
			     :accessor prepared-statement-counter-of)))

(defun prepare-statement (session query)
  (funcall
   (prepare-statement-function-of
    (configuration-of session))
   (connection-of session)
   (format nil "prepaired_statement_~a"
	   (incf (prepared-statement-counter-of session)))
   query))

(defvar *session*)

(defun call-with-session (session function)
  (let ((*session* session))
    (funcall function)))

(defmacro with-session ((&optional (session *default-session*))
			&body body)
  `(call-with-session ,session #'(lambda () ,@body)))

(defmacro db-query (bindings &optional options-and-clauses
		    &body select-list)
  (let ((compiled-bindings (apply #'compile-bindings bindings)))
    `(make-query ,@(apply #'compile-select-list
			  compiled-bindings
			  select-list)
		 :limit (apply #'get-option :limit
			       options-and-clauses)
		 :offset (apply #'get-option :offset
				options-and-clauses)
		 :single (apply #'get-option :single
				options-and-clauses)
		 :fetch ,@(apply #'compile-fetch-clause
				 compiled-bindings
				 (apply #'get-option :fetch
					options-and-clauses))
		 :where ,@(apply #'compile-where-clause
				 compiled-bindings
				 (apply #'get-option :where
					options-and-clauses))
		 :having ,@(apply #'compile-having-clause
				  compiled-bindings
				  (apply #'get-option :having
					 options-and-clauses))
		 :order-by ,@(apply #'compile-order-by-clause
				    compiled-bindings
				    (apply #'get-option :order-by
					   options-and-clauses)))))

;; (db-persist object)
;; (db-remove object)

;; connection name parameters
(defclass joined-superclass ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (table-alias :initarg :table-alias
		:reader table-reference-of)
   (joined-superclasses :initarg :joined-superclasses
			:reader joined-superclasses-of)
   (joined-fetchings :initarg :joined-fetchings
		     :reader joined-fetchings-of)))

(defmethod initialize-instance :after ((instance joined-superclass)
				       &key class-mapping
				       (joined-superclasses
					(compute-joined-superclasses
					 (superclasses-mappings-of class-mapping))))
  (setf (slot-value instance 'joined-superclasses) joined-superclasses))

(defclass object-loader (joined-superclass)
  ((subclass-object-loaders :initarg :subclass-object-loaders
			    :reader subclass-object-loaders-of)))

(defun compute-joined-superclasses (superclasses-mappings)
  (mapcar #'(lambda (superclass-mapping)
	      (make-instance 'joined-superclass
			     :class-mapping superclass-mapping))
	  superclasses-mappings))

(defun compute-subclass-object-loaders (object-loader class-mapping)
  (mapcar #'(lambda (subclass-mapping)
	      (make-instance 'object-loader
			     :class-mapping subclass-mapping
			     :joined-superclasses (list* object-loader
							 (compute-joined-superclasses
							  (remove class-mapping
								  (superclasses-mappings-of subclass-mapping))))))
	  (subclasses-mappings-of class-mapping)))

(defmethod initialize-instance :after ((instance object-loader)
				       &key class-mapping)
  (setf (slot-value instance 'subclass-object-loaders)
	(compute-subclass-object-loaders instance class-mapping)))

(defun make-object-loader (class-mapping joined-fetchings)
  (declare (ignore joined-fetchings))
  (make-instance 'object-loader
		 :class-mapping class-mapping
		 :joined-superclasses (compute-joined-superclasses
				       (superclasses-mappings-of class-mapping))))

