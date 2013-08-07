;;; cl-db.lisp

(in-package #:cl-db)

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
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (columns :initarg :columns :reader columns-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defclass value-mapping-definition
    (slot-mapping-definition)
  ())

(defclass reference-mapping-definition (slot-mapping-definition)
  ((mapped-class :initarg :mapped-class :reader mapped-class-of)))

(defclass many-to-one-mapping-definition
    (reference-mapping-definition)
  ())

(defclass one-to-many-mapping-definition
    (reference-mapping-definition)
  ())

;; добавить вычисление отображения слотов класса по всей иерархии
;; effective-slot-mapping; реализовать слот "расстояния" до
;; отображения слота, как выражения объединения таблиц (?)
;; рассмотреть случай вычисления дополнительных присоединений к
;; цепочке таблиц

(defclass table ()
  ((name :initarg :name
	 :reader name-of)
   (columns :initarg :columns
	    :reader columns-of)
   (schema-name :initarg :schema-name
		:reader schema-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)))

(defclass column ()
  ((name :initarg :name
	 :reader name-of)
   (sql-type :initarg :sql-type
	     :reader sql-type-of)))

(defclass unique-constraint ()
  ((name :initarg :name
	 :reader name-of)
   (columns :initarg :columns
	    :reader columns-of)))

(defclass foreign-key ()
  ((name :initarg :name
	 :reader name-of)
   (table :initarg :table
	  :reader table-of)
   (columns :initarg :columns
	    :reader columns-of)
   (referenced-table :initarg :referenced-table
		     :reader referenced-table-of)))

(defclass class-mapping ()
  ((table :initarg :table
	  :reader table-of)
   (mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (value-mappings :initarg :value-mappings
		   :reader value-mappings-of)
   (reference-mappings :initform (list)
		       :accessor reference-mappings-of)
   (subclasses-mappings :initform (list)
			:accessor subclasses-mappings-of)
   (superclasses-mappings :initform (list)
			  :accessor superclasses-mappings-of)))

(defclass foreign-key-mapping ()
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass association (foreign-key-mapping)
  ((many-to-one-mapping :initarg :many-to-one-mapping
			:reader many-to-one-mapping-of)
   (one-to-many-mapping :initarg :one-to-many-mapping
			:reader one-to-many-mapping-of)))

(defclass inheritance-mapping (foreign-key-mapping)
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

(defclass superclass-mapping (inheritance-mapping)
  ())

(defclass subclass-mapping (inheritance-mapping)
  ())

(defclass slot-mapping ()
  ((path :initarg :path :reader path-of)
   (slot-name :initarg :slot-name :reader slot-name-of)
   (marshaller :initarg :marshaller :reader marshaller-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)))

(defclass value-mapping (slot-mapping)
  ((columns :initarg :columns :reader columns-of)))

(defclass reference-mapping (slot-mapping)
  ((association :initarg :association
		:reader association-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

(defclass many-to-one-mapping (reference-mapping) ())

(defclass one-to-many-mapping (reference-mapping) ())

(defclass mapping-schema ()
  ((tables :initarg :tables
	   :reader tables-of)
   (class-mappings :initarg :class-mappings
		   :reader class-mappings-of)))

(defmethod initialize-instance :after ((instance mapping-schema)
				       &key class-mappings)
  (let ((mappings-table
	 (alexandria:alist-hash-table
	  (mapcar #'(lambda (class-mapping)
		      (cons (mapped-class-of class-mapping)
			    class-mapping))
		  class-mappings))))
    (with-slots (class-mappings) instance
      (setf class-mappings mappings-table))))

(defun get-class-mapping (mapped-class mapping-schema)
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class (class-mappings-of mapping-schema))
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defun get-slot-name (class reader)
  (let ((reader-name (generic-function-name reader)))
    (loop for slot in (class-slots class)
       when (find reader-name (slot-definition-readers slot))
       return (slot-definition-name slot)
       finally (error "Slot for reader ~a not found" reader-name))))

(defun get-reference-mapping (class-mapping reference-reader)
  (multiple-value-bind (reference-mapping presentp)
      (gethash
       (get-slot-name (mapped-class-of class-mapping)
		      reference-reader)
       (reference-mappings-of class-mapping))
    (if (not presentp)
	(error "Mapped reference for accessor ~a not found"
	       reference-reader)
	reference-mapping)))

(defun make-mapping-schema (&rest class-mapping-definitions)
  (let* ((tables (apply #'make-tables
			class-mapping-definitions))
	 (class-mappings (apply #'make-class-mappings
				tables class-mapping-definitions)))
    (compute-associations class-mappings
				class-mapping-definitions)
    (compute-
    (dolist (class-mapping class-mappings)
      (append
       (loop for association in associations collect
	    (or (when (eq (class-mapping-of
			   (many-to-one-of association))
			  class-mapping)
		  
       (remove class-mapping associations
	 
			      
    