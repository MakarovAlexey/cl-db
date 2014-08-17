;;; cl-db.lisp

(in-package #:cl-db)

(defclass persistent-class (standard-class)
  ((table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (superclass-custom-columns :initarg :superclass-custom-columns
			      :reader superclass-custom-columns-of)
   (inheritance-mappings :reader inheritance-mappings-of)
   (properties :initform (make-hash-table)
	       :reader properties-of
	       :documentation "Properties by name")
   (references :initform (make-hash-table)
	       :reader references-of
	       :documentation "Many-to-one and one-to-many references by name")))

(defmethod validate-superclass ((class persistent-class)
				(superclass standard-class))
  t)

(defclass persistent-object ()
  ())

(defmethod initialize-instance :around
    ((class persistent-class)
     &rest initargs &key direct-superclasses)
  (declare (dynamic-extent initargs))
  (if (loop for class in direct-superclasses
            thereis (subtypep class (find-class 'persistent-object)))
      (call-next-method)
      (apply #'call-next-method class :direct-superclasses
	     (append direct-superclasses
		     (list (find-class 'persistent-object)))
	     initargs)))

(defmethod reinitialize-instance :around
    ((class persistent-class) &rest initargs
     &key (direct-superclasses '() direct-superclasses-p))
  (declare (dynamic-extent initargs))
  (when direct-superclasses-p
    (if (loop for class in direct-superclasses
	   thereis (subtypep class (find-class 'persistent-object)))
	(call-next-method)
	(apply #'call-next-method class :direct-superclasses
	       (append direct-superclasses
		       (list (find-class 'persistent-object)))
	       initargs))
    (call-next-method)))

(defclass persistent-slot-definition (standard-slot-definition)
  ((columns :initarg :columns
	    :reader columns-of)))

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
  ((index-fn :initarg :index-by
	     :reader index-fn-of)))

;;(defmethod effective-slot-definition-class
;;    ((class persistent-class) &key name &allow-other-keys)
;;  (case (mapping-type-of
;;	 (find name (class-direct-slots class)
;;	       :key #'slot-definition-name))
;;    (:property 'property-effective-slot-definition)
;;    (:one-to-many 'one-to-many-effective-slot-definition)
;;    (:many-to-one 'many-to-one-effective-slot-definition)
;;    (t 'standard-effective-slot-definition)))

;;(defgeneric index-slot-by-readers (slot class))

;;(defmethod index-slot-by-readers
;;    ((slot property-effective-slot-definition) class)
;;  (loop for reader in (slot-definition-readers slot)
;;     do (setf (gethash reader (properties-of class)) slot)))

;;(defmethod index-slot-by-readers
;;    ((slot reference-effective-slot-definition) class)
;;  (loop for reader in (slot-definition-readers slot)
;;     do (setf (gethash reader (references-of class)) slot)))

(defclass inheritance-mapping ()
  ((superclass :initarg :superclass
	       :reader superclass-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defmethod finalize-inheritance :after ((class persistent-class))
  (loop for slot in (class-slots class)
     do (index-slot-by-readers slot class))
  (with-slots (inheritance-mappings) class
    (setf inheritance-mappings
	  (loop for superclass in (class-direct-superclasses class)
	     for foreign-key = (or (rest (assoc
					  (class-name superclass)
					  (superclass-custom-columns-of class)))
				   (primary-key-of superclass))
	     when (subtypep superclass (class-of class))
	     collect (make-instance 'inheritance-mapping
				    :superclass superclass
				    :foreign-key foreign-key)))))
				    
					
