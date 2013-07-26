(in-package #:cl-db)

;; query language

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

;; (defmacro db-query (bindings options &body row))
;; bindings => from clause
;; row => select list
;; options => optional 

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

