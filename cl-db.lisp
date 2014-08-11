;;; cl-db.lisp

(in-package #:cl-db)

(defclass persistent-class (standard-class)
  ((table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (superclass-custom-columns :initarg :superclass-custom-columns
			      :reader superclass-custom-columns-of)
   (inheritance-foreign-keys :reader superclass-foreign-keys-of)
   (properties :initform (make-hash-table)
	       :reader properties-of
	       :documntation "Properties by name")
   (references :initform (make-hash-table)
	       :reader references-of
	       :documntation "Many-to-one and one-to-many references by name")))

(defmethod validate-superclass ((class persistent-class)
				(superclass standard-class))
  t)

(defclass persistent-object ()
  ())

(defmethod initialize-instance :around
    ((class persistent-class)
     &rest initargs &key direct-superclasses)
  (declare (dynamic-extent initargs))x
  (if (loop for class in direct-superclasses
            thereis (subtypep class (find-class 'persistent-object)))
      (call-next-method)
      (apply #'call-next-method class :direct-superclasses
	     (list* (find-class 'persistent-object)
		    direct-superclasses)
	     initargs)))

(defmethod reinitialize-instance :around
    ((class persistent-class) &rest initargs
     &key (direct-superclasses '() direct-superclasses-p))
  (declare (dynamic-extent initargs))
  (when direct-superclasses-p
    (if (loop for class in direct-superclasses
	   thereis (subtypep class (find-class 'persistent-object)))
	(call-next-method)
	(apply #'call-next-method
	       class
	       :direct-superclasses
	       (list* (find-class 'persistent-object)
		      direct-superclasses)
	       initargs))
    (call-next-method)))

(defclass persistent-slot-definition (standard-slot-definition)
  ((columns :initarg :columns :reader columns-of)))

(defclass persistent-direct-slot-definition
    (persistent-slot-definition standard-direct-slot-definition)
  ((mapping-type :initarg :mapping-type
		 :reader mapping-type-of)
   (referenced-class :initarg :referenced-class
		     :reader referenced-class-of)
   (index-fn :initarg :index-by
	     :reader index-fn)))

(defmethod direct-slot-definition-class
    ((class persistent-class) &key &allow-other-keys)
  'persistent-direct-slot-definition)

(defclass property-effective-slot-definition
    (persistent-slot-definition standard-effective-slot-definition)
  ())

(defclass reference-effective-slot-definition
    (persistent-slot-definition standard-effective-slot-definition)
  ((referenced-class :initarg :referenced-class
		     :reader referenced-class-of)))

(defclass many-to-one-effective-slot-definition
    (reference-effective-slot-definition standard-effective-slot-definition)
  ())

(defclass one-to-many-effective-slot-definition
    (reference-effective-slot-definition standard-effective-slot-definition)
  ((index-fn :initarg :index-by :reader index-fn-of)))

(defmethod effective-slot-definition-class
    ((class persistent-class) &key mapping-type &allow-other-keys)
  (ecase mapping-type
    (:property 'property-effective-slot-definition)
    (:one-to-many 'one-to-many-effective-slot-definition)
    (:many-to-one 'many-to-one-effective-slot-definition)))

(defun index-slot (table direct-slot)
  (loop reader in (slot-definition-readers direct-slot)
     do (setf (gethash reader table) direct-slot)))

(defmethod finalize-inheritance :after ((class persistent-class))
  (loop for direct-slot in (class-direct-slots class)
     do (case (mapping-type-of direct-slot)
	  (:property
	   (index-slot (properties-of class) direct-slot))
	  (:many-to-one
	   (index-slot (references-of class) direct-slot))
	  (:one-to-many
	   (index-slot (references-of class) direct-slot)))))
