;;; cl-db.lisp

(in-package #:cl-db)

(defclass class-mapping ()
  ((class-name :initarg :class-name
	       :reader class-name-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (superclass-mappings :initarg :superclass-mapping
			:reader superclass-mappings-of)
   (subclass-mappings :initarg :subclass-mappings
		      :reader subclass-mappings-of)))

(defclass inheritance-mapping ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass slot-mapping ()
  ((slot-name :initarg :class-name
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

(defvar *class-mappings*)

(defun parse-slot-mappings (&rest slot-mappings)
  (loop for slot-mapping in slot-mappings collect
       (destructuring-bind
	     (slot-name (mapping-type &rest params)
			&optional deserializer serializer)
	   slot-mapping
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
			   :reference-class-name (first params)
			   :foreign-key (rest params)))
	   (:one-to-many
	    (make-inastance 'one-to-many
			    :slot-name slot-name
			    :reference-class-name (first params)
			    :foreign-key (rest params)))))))

(defun parse-class-mapping (class-mapping)
  (destructuring-bind
	(class-name ((table-name &rest primary-key)
		     &rest superclasses)
		    &rest slot-mappings)
      class-mapping
    (let ((slot-mappings
	   (apply #'parse-slot-mappings slot-mappings)))
      (list :class-name class-name
	    :table-name table-name
	    :primary-key primary-key
	    :superclass-mappings superclasses
	    :property-mappings (mapcar #'rest
				       (remove-if-not
					#'(lambda (mapping)
					    (eq (first mapping) :property))
					slot-mappings))
	    :many-to-one-mappings (mapcar #'rest
					  (remove-if-not
					   #'(lambda (mapping)
					       (eq (first mapping) :many-to-one))
					   slot-mappings))
	    :one-to-many-mappings (mapcar #'rest
					  (remove-if-not
					   #'(lambda (mapping)
					       (eq (first mapping) :one-to-many))
					   slot-mappings))))))

(defun find-class-mapping (class-name &optional (class-mappings
						 *class-mappings*))
  (find class-name class-mappings
	:key #'(lambda (class-mapping)
		 (getf class-mapping :class-name))))

(defun compute-superclass-mappings (&rest superclass-mappings)
  (mapcar #'(lambda (superclass-mapping)
	      (destructuring-bind (class-name &rest foreign-key)
		  superclass-mapping
		(list* (apply #'compute-inheritance-mapping
			      (find-class-mapping class-name))
		       foreign-key)))
	  superclass-mappings))

(defun compute-inheritance-mapping (&key class-name superclass-mappings
				      table-name primary-key 
				      property-mappings
				      many-to-one-mappings
				      one-to-many-mappings)
  (list* class-name
	 (list* table-name primary-key property-mappings)
;;	  :many-to-one-mappings many-to-one-mappings
;;	 :one-to-many-mappings one-to-many-mappings)
	 (apply #'compute-superclass-mappings superclass-mappings)))

(defun compute-subclass-mapping (root-class-name
				 &key class-name superclass-mappings
				   table-name primary-key 
				   property-mappings
				   many-to-one-mappings
				   one-to-many-mappings)
  (let ((root-superclass
	 (assoc root-class-name superclass-mappings)))
    (apply #'(lambda (superclass-name &rest foreign-key)
	       (declare (ignore superclass-name))
	       (list* (compute-class-mapping :class-name class-name
					     :table-name table-name
					     :primary-key primary-key
					     :property-mappings property-mappings
					     :many-to-one-mappings many-to-one-mappings
					     :one-to-many-mappings one-to-many-mappings
					     :superclass-mappings
					     (remove root-superclass
						     superclass-mappings))
		      foreign-key))
	   root-superclass)))

(defun compute-subclass-mappings
    (class-name &optional (class-mappings *class-mappings*))
  (mapcar #'(lambda (subclass-mapping)
	      (apply #'compute-subclass-mapping
		     class-name subclass-mapping))
	  (remove-if-not #'(lambda (subclass-mapping)
			     (apply #'(lambda (&key superclass-mappings
						 &allow-other-keys)
					(find class-name
					      superclass-mappings
					      :key #'first))
				    subclass-mapping))
			 class-mappings)))

(defun compute-class-mapping (&rest class-mapping
			      &key class-name &allow-other-keys)
  (list*
   (apply #'compute-inheritance-mapping class-mapping)
   (compute-subclass-mappings class-name)))

(defmacro define-schema (name params &rest class-mappings)
  (declare (ignore params))
  `(defun ,name ()
     ,(let ((*class-mappings*
	     (mapcar #'parse-class-mapping class-mappings)))
	   `(quote ,(mapcar #'(lambda (class-mapping)
				(apply #'compute-class-mapping class-mapping))
			    *class-mappings*)))))

;;(defmacro abs (class-name
;;	       ((((table-name primary-key &rest subclass-mappings)
;;		  &rest one-to-many-mappings)
;;		 &rest many-to-one-mappings)
;;		&rest value-mappings)
;;	       &rest superclass-mappings))
