;;; cl-db.lisp

(in-package #:cl-db)

(defvar *mapping-schema-definitions* (make-hash-table))

(defvar *class-mapping-definitions*)

(defclass column-definition ()
  ((column-name :initarg :column-name
		:reader column-name-of)
   (column-type :initarg :column-type
		:reader column-type-of)))

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (superclasses :initarg :superclasses
		 :reader superclasses-of)
   (value-mappings :initarg :value-mappings
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)))

(defclass slot-mapping-definition ()
  ((slot-name :initarg :slot-name
	      :reader slot-name-of)
   (unmarshaller :initarg :unmarshaller
		 :reader unmarshaller-of)
   (marshaller :initarg :marshaller
	       :reader marshaller-of)))

(defclass value-mapping-definition
    (slot-mapping-definition)
  ((columns :initarg :columns
	    :reader columns-of)))

(defclass reference-mapping-definition
    (slot-mapping-definition)
  ((columns-names :initarg :columns-names
		  :reader columns-names-of)
   (referenced-class :initarg :referenced-class
		     :reader referenced-class-of)))

(defclass many-to-one-mapping-definition
    (reference-mapping-definition)
  ())

(defclass one-to-many-mapping-definition
    (reference-mapping-definition)
  ())

(define-condition mapping-schema-redefinition (style-warning)
  ((name :initarg :name :reader name-of)))

(defun compile-option (option options)
  (rest (assoc option options)))

(defun ensure-mapping-schema-definition (name &optional
					 (mapping-schema-definitions
					  *mapping-schema-definitions*))
  (multiple-value-bind (mapping-schema presentp)
      (gethash name mapping-schema-definitions)
    (declare (ignore mapping-schema))
    (when presentp
      (warn 'mapping-schema-redefinition :name name)))
  (let ((class-mapping-definitions (make-hash-table)))
    (setf (gethash name mapping-schema-definitions)
	  class-mapping-definitions
	  *class-mapping-definitions*
	  class-mapping-definitions))
  name)

(defmacro define-mapping-schema (name)
  `(ensure-mapping-schema-definition (quote ,name)))

(defun assert-duplicate-slot-mappings (slot-mappings)
  (reduce #'(lambda (slot-names slot-name)
	      (if (find slot-name slot-names)
		  (error "Duplicate slot mapping: ~a" slot-name)
		  (list* slot-name slot-names)))
	  slot-mappings :key #'first
	  :initial-value (list)))

(defun make-value-mapping (slot-name marshaller unmarshaller
			   column &rest columns)
  `(make-instance 'value-mapping-definition
		  :slot-name (quote ,slot-name)
		  :marshaller ,marshaller
		  :unmarshaller ,unmarshaller
		  :columns (list
			    ,@(loop for (name sql-type)
				 in (list* column columns)
				 collect `(make-instance 'column-definition
							 :column-name ,name
							 :column-type ,sql-type)))))

(defun make-many-to-one-mapping (slot-name marshaller unmarshaller
				 mapped-class column &rest columns)
  `(make-instance 'many-to-one-mapping-definition
		  :slot-name (quote ,slot-name)
		  :referenced-class (find-class (quote ,mapped-class))
		  :columns-names (quote ,(list* column columns))
		  :marshaller ,marshaller
		  :unmarshaller ,unmarshaller))

(defun make-one-to-many-mapping (slot-name marshaller unmarshaller
				 mapped-class column &rest columns)
  `(make-instance 'one-to-many-mapping-definition
		  :slot-name (quote ,slot-name)
		  :referenced-class (find-class (quote ,mapped-class))
		  :columns-names (quote ,(list* column columns))
		  :marshaller ,marshaller
		  :unmarshaller ,unmarshaller))

(defun compile-slot-mappings (type function &rest slot-mappings)
  (let ((mappings (list)))
    (dolist (slot-mapping slot-mappings)
      (destructuring-bind
	    (slot-name (mapping-type &rest options)
		       &optional marshaller unmarshaller)
	  slot-mapping
	(when (eq mapping-type type)
	  (push (apply function slot-name
		       marshaller unmarshaller options)
		mappings))))
    (reverse mappings)))

(defun ensure-class-mapping-definition
    (class table-name primary-key superclasses value-mappings
     many-to-one-mappings one-to-many-mappings &optional
     (class-mapping-definitions *class-mapping-definitions*))
  (finalize-inheritance class)
  (setf (gethash class class-mapping-definitions)
	(make-instance 'class-mapping-definition
		       :mapped-class class
		       :table-name table-name
		       :primary-key primary-key
		       :superclasses superclasses
		       :value-mappings value-mappings
		       :many-to-one-mappings many-to-one-mappings
		       :one-to-many-mappings one-to-many-mappings)))

(defmacro define-class-mapping ((class-name table-name)
				options	&body slot-mappings)
  (assert-duplicate-slot-mappings slot-mappings)
  `(ensure-class-mapping-definition
    (find-class (quote ,class-name))
    ,table-name
    (quote ,(compile-option :primary-key options))
    (mapcar #'find-class
	    (quote ,(compile-option :superclasses options)))
    (list
     ,@(apply #'compile-slot-mappings :value
	      #'make-value-mapping slot-mappings))
    (list
     ,@(apply #'compile-slot-mappings :many-to-one
	      #'make-many-to-one-mapping slot-mappings))
    (list
     ,@(apply #'compile-slot-mappings :one-to-many
	      #'make-one-to-many-mapping slot-mappings))))