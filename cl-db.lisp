;;; cl-db.lisp

(in-package #:cl-db)

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initarg :columns :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initform (list) :reader foreign-keys-of)))

(defclass column ()
  ((name :initarg :name :reader name-of)
   (type-name :initarg :type-name :reader type-name-of)))

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

(defclass inheritance-mapping (foreign-key-mapping)
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

(defclass superclass-mapping (inheritance-mapping)
  ())

(defclass subclass-mapping (inheritance-mapping)
  ())

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defclass value-mapping (slot-mapping)
  ((columns :initarg :columns :reader columns-of)))

(defclass reference-mapping
    (slot-mapping foreign-key-mapping)
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

;;  ((association :initarg :association
;;		:reader association-of)

(defclass one-to-many-mapping (reference-mapping) ())

(defclass many-to-one-mapping (reference-mapping) ())

(defclass mapping-schema ()
  ((tables :initarg :tables
	   :reader tables-of)
   (class-mappings :reader class-mappings-of)))

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

(defun get-mapping (mapped-class mapping-schema)
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class (class-mappings-of mapping-schema))
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

;;(defclass association (foreign-key-mapping)
;;  ())

;;(defclass unidirectional-many-to-one-association (association)
;;  ((many-to-one-direction :initarg :many-to-one-direction
;;			  :reader many-to-one-direction-of)))

;;(defclass unidirectional-one-to-many-association (association)
;;  ((one-to-many-direction :initarg :one-to-many-direction
;;			  :reader one-to-many-direction-of)))

;;(defclass bidirectional-association
;;    (unidirectional-many-to-one-association
;;     unidirectional-one-to-many-association)
;;  ())