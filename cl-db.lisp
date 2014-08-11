;;; cl-db.lisp

(in-package #:cl-db)

(defclass persistent-class (standard-class)
  ((table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (superclass-columns :initarg :superclass-mappings
		       :reader superclass-mappings-of)))

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

(defmethod compute-effective-slot-definition
    ((class persistent-class) name direct-slot-definitions)
  (call-next-method)
  (class-precedence-list 

(defun find-class-mapping (class-name &optional (class-mappings
						 *class-mappings*))
  (find class-name class-mappings
	:key #'(lambda (class-mapping)
		 (getf :class-name class-mapping))))

(defun compute-superclass-mappings (class-name &rest foreign-key)
    (list :class-name class-name
	  :table-name table-name
	  :primary-key primary-key
	  :foreign-key foreign-key
	  :superclasses (mapcar #'(lambda (superclass-mapping)
				    (apply #'compute-superclass-mappings
					   superclass-mapping))
				superclasses)))

(defun compute-subclass-mapping (root-name &key class-name table-name
					     primary-key superclasses
					     value-mappings
					     many-to-one one-to-many)
  (list* :foreign-key (rest (find root-name superclasses :key #'first))
	 (apply #'compute-class-mapping
		:class-name class-name
		:table-name table-name
		:primary-key primary-key
		:superclassses (remove root-name superclasses :key #'first)
		:value-mappings value-mappings
		:many-to-one many-to-one
		:one-to-many one-to-many)))

(defun compute-class-mapping (&key class-name table-name primary-key
				superclasses value-mappings
				many-to-one one-to-many)
  (list :class-name class-name
	:table-name table-name
	:primary-key primary-key
	:superclasses (mapcar #'(lambda (superclass-mapping)
				  (apply #'compute-superclass-mapping
					 superclass-mapping))
			      superclasses)
	:subclasses (mapcar #'(lambda (class-mapping)
				(apply #'compute-subclass-mapping
				       class-name class-mapping))
			    (remove-if-not #'(lambda (class-mapping)
					       (find class-name
						     (getf class-mapping :superclasses)))
					   *class-mappings*))))

(defmacro define-schema (name params &rest class-mappings)
  (declare (ignore params))
  `(let ((*class-mappings*
	  (quote ,(apply #'parse-mappings class-mappings))))
     (mapcar #'(lambda (class-mapping)
		 (apply #'compute-class-mapping class-mapping))
	     *class-mappings*)))
