;;; cl-db.lisp

(in-package #:cl-db)

(defparameter *schema-table* (make-hash-table))

(defvar *mapping-schema*)

(defun schema-table ()
  *schema-table*)

(defun current-schema ()
  *mapping-schema*)

(defun register-mapping-schema (table schema-name)
  (setf (gethash schema-name table) (make-hash-table)))

(defmacro define-mapping-schema (name)
  `(register-mapping-schema (schema-table) (quote ,name)))

(defun activate-schema (name)
  (setf *mapping-schema*
	(gethash name (schema-table))))

(defmacro use-mapping-schema (name)
  `(activate-schema (quote ,name)))

(defclass class-mapping ()
  ((mapping-schema :initarg :mapping-schema
		   :reader mapping-schema-of)
   (class-name :initarg :class-name
	       :reader class-name-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (superclass-mappings :initform (list)
			:accessor superclass-mappings-of)
   (subclass-mappings :initform (list)
		      :accessor subclass-mappings-of)
   (value-mappings :initarg :value-mappings
		   :reader value-mappings-of)
   (reference-mappings :initarg :reference-mappings
		       :reader references-mappings-of)))

(defclass inheritance-mapping ()
  ((subclass-mapping :initarg :subclass-mapping
		  :reader subclass-mapping-of)
   (superlass-mapping :initarg :superclass-mapping
		      :reader superclass-mapping-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name
	      :reader slot-name-of)))

(defclass value-mapping (slot-mapping)
  ((columns :initarg :columns
	    :reader columns-of)
   (serializer :initarg :serializer
	       :reader serializer-of)
   (deserializer :initarg :deserializer
		 :reader deserializer-of)))

(defclass reference-mapping (slot-mapping)
  ((referenced-class-name :initarg :referenced-class-name
			  :reader referenced-class-name-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass many-to-one-mapping (reference-mapping)
  ())

(defclass one-to-many-mapping (reference-mapping)
  ())

(defun parse-slot-mapping (slot-name mapping
			   &optional deserializer serializer)
  (destructuring-bind (mapping-type &rest params)
      mapping
    (case mapping-type
      (:property
       (make-instance 'value-mapping
			   :slot-name slot-name
			   :columns params
			   :serializer serializer
			   :deserializer deserializer))
      (:many-to-one
       (make-instance 'many-to-one-mapping
		      :slot-name slot-name
		      :referenced-class-name (first params)
		      :foreign-key (rest params)))
      (:one-to-many
       (make-instance 'one-to-many-mapping
		       :slot-name slot-name
		       :referenced-class-name (first params)
		       :foreign-key (rest params))))))

(defun parse-slot-mappings (slot-mappings)
  (loop for slot-mapping-expression in slot-mappings
     for slot-mapping = (apply #'parse-slot-mapping
			       slot-mapping-expression)
     when (subtypep (type-of slot-mapping) 'value-mapping)
     collect it into value-mappings
     when (subtypep (type-of slot-mapping) 'reference-mapping)
     collect it into reference-mappings
     do (values value-mappings reference-mappings)))

(defun get-class-mapping (class-name &optional (mapping-schema
						 *mapping-schema*))
  (gethash class-name mapping-schema))

(defun register-inheritance (class-mapping superclass-name foreign-key)
  (let* ((superclass-mapping (get-class-mapping superclass-name))
	 (inheritance-mapping
	  (make-instance 'inheritance-mapping
			 :superclass-mapping superclass-mapping
			 :subclass-mapping class-mapping
			 :foreign-key foreign-key)))
    (push inheritance-mapping
	  (subclass-mappings-of superclass-mapping))
    (push inheritance-mapping
	  (superclass-mappings-of class-mapping))))

(defun register-superclass-mappings (class-mapping superclass-mappings)
  (multiple-value-bind (class-mapping presentp)
      (get-class-mapping (class-name-of class-mapping)
			 (mapping-schema-of class-mapping))
    (when presentp
      (dolist (inheritance-mapping
		(superclass-mappings-of class-mapping))
	(let ((superclass-mapping
	       (superclass-mapping-of inheritance-mapping)))
	  (setf (subclass-mappings-of superclass-mapping)
		(remove inheritance-mapping
			(subclass-mappings-of superclass-mapping)))))))
  (dolist (superclass-mapping superclass-mappings)
    (destructuring-bind (superclass-name &rest foreign-key)
	superclass-mapping
      (register-inheritance class-mapping superclass-name
			    foreign-key))))

(defun register-subclass-mappings (class-mapping)
  (multiple-value-bind (previous-class-mapping presentp)
      (get-class-mapping (class-name-of class-mapping)
			 (mapping-schema-of class-mapping))
    (when presentp
      (dolist (inheritance-mapping
		(subclass-mappings-of previous-class-mapping))
	(let* ((subclass-mapping
		(subclass-mapping-of inheritance-mapping))
	       (new-inheritance-mapping
		(make-instance 'inheritance-mapping
			       :superclass-mapping class-mapping
			       :subclass-mapping subclass-mapping
			       :foreign-key
			       (foreign-key-of inheritance-mapping))))
	  (setf
	   (superclass-mappings-of subclass-mapping)
	   (list* new-inheritance-mapping
		  (remove inheritance-mapping
			  (superclass-mappings-of subclass-mapping))))
	  (push (subclass-mappings-of class-mapping)
		new-inheritance-mapping))))))

(defun register-class-mapping (class-mapping superclass-mappings)
  (let ((mapping-schema (mapping-schema-of class-mapping)))
    (register-superclass-mappings class-mapping superclass-mappings)
    (register-subclass-mappings class-mapping)
    (setf (gethash (class-name-of class-mapping)
		   mapping-schema)
	  class-mapping)))
	  

(defun make-class-mapping (class-name table-name primary-key
			   superclass-mappings slot-mappings)
  (let ((mapping-schema (current-schema)))
    (multiple-value-bind (value-mappings reference-mappings)
	(parse-slot-mappings slot-mappings)
      (register-class-mapping
       (make-instance 'class-mapping
		      :class-name class-name
		      :mapping-schema mapping-schema
		      :table-name table-name
		      :primary-key primary-key
		      :value-mappings value-mappings
		      :reference-mappings reference-mappings)
       superclass-mappings))))

(defmacro define-class-mapping (class-name
				((table-name &rest primary-key)
				 &rest superclass-mappings)
				&rest slot-mappings)
  `(make-class-mapping (quote ,class-name)
		       ,table-name
		       (quote ,primary-key)
		       (quote ,superclass-mappings)
		       (quote ,slot-mappings)))
