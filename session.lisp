(in-package #:cl-db)

(defclass clos-session ()
  ((mappings :initarg :mappings :reader mappings-of)
   (connection :initarg :connection :reader connection-of)
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defvar *default-session*)

(defmacro define-session ((name mapping-schema)
			  (&key (default t)) &body body)
  `(progn (defun ,name ()
	    (make-instance 'clos-session
			   :mappings (funcall
				      (function ,mapping-schema))
			   :connection ,@body))
	  ,@(when default
		  `((setf *default-session* (function ,name))))))



(defun make-clos-session (connector mapping-schema)
  (make-instance 'clos-session
		 :connection (funcall connector)
		 :mapping-schema mapping-schema))

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

(defvar *session*)

(defun call-with-session (session function)
  (let ((*session* session))
    (funcall function)))

(defmacro with-session ((session) &body body)
  `(call-with-session ,session #'(lambda () ,@body)))