;;; cl-db.lisp

(in-package #:cl-db)

(defvar *class-mappings*)

(defvar *session*)

(defun parse-slot-mappings (&rest slot-mappings)
  (loop for slot-mapping in slot-mappings collect
       (destructuring-bind
	     (slot-name (mapping-type &rest params)
			&optional deserializer serializer)
	   slot-mapping
	 (case mapping-type
	   (:property
	    (list* :property slot-name params))
	   (:many-to-one
	    (list :many-to-one slot-name
		  :reference-class-name (first params)
		  :foreign-key (rest params)))
	   (:one-to-many
	    (list :one-to-many slot-name
		  :reference-class-name (first params)
		  :foreign-key (rest params)
		  :serializer serializer
		  :deserializer deserializer))))))

(defun column-of (property)
  (third property))

(defun slot-name-of (slot-mapping)
  (second slot-mapping))

(defun value-of (value-mapping)
  (rest value-mapping))

(defun mapping-of (value-mapping)
  (first value-mapping))

(defun reference-class-of (slot-mapping)
  (getf slot-mapping :reference-class-name))

(defun mapping-name-of (mapping)
  (getf mapping :class-name))

(defun property-mappings-of (class-mapping)
  (getf class-mapping :properties))

(defun class-name-of (class-mapping)
  (getf class-mapping :class-name))

(defun subclass-mappings-of (class-mapping)
  (getf class-mapping :subclass-mappings))

(defun many-to-one-mappings-of (class-mapping)
  (getf class-mapping :many-to-one-mappings))

(defun superclass-mappings-of (class-mapping)
  (getf class-mapping :superclass-mappings))

(defun one-to-many-mappings-of (class-mapping)
  (getf class-mapping :one-to-many-mappings))

(defun inverted-one-to-many-mappings-of (class-mapping)
  (getf class-mapping :inverted-one-to-many))

(defun table-name-of (class-mapping)
  (getf class-mapping :table-name))

(defun serializer-of (one-to-many-mapping)
  (eval (getf one-to-many-mapping :serializer)))

(defun primary-key-of (class-mapping)
  (getf class-mapping :primary-key))

(defmethod foreign-key-of ((reference-mapping list))
  (getf reference-mapping :foreign-key))

(defgeneric columns-of (object))

(defmethod columns-of ((class-mapping list))
  (getf class-mapping :columns))

;;(defun cascade-operation-of (reference-mapping)
;;  (let ((operation-type (getf reference-mapping :delete-orphan)))
;;    (if (eq operation-type :delete-orphan)
;;	#'delete-orphaned ; generic-functions
;;	#'update-orphaned)))

(defun get-class-mapping (class-name &optional (session *session*))
  (or
   (find class-name (mapping-schema-of session) :key #'class-name-of)
   (error "class mapping for class ~a not found" class-name)))

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
	    :properties (remove-if-not
			 #'(lambda (mapping)
			     (eq (first mapping) :property))
			 slot-mappings)
	    :many-to-one-mappings (remove-if-not
				   #'(lambda (mapping)
				       (eq (first mapping) :many-to-one))
				   slot-mappings)
	    :one-to-many-mappings (remove-if-not
				   #'(lambda (mapping)
				       (eq (first mapping) :one-to-many))
				   slot-mappings)))))

(defun find-class-mapping (class-name &optional (class-mappings
						 *class-mappings*))
  (find class-name class-mappings
	:key #'(lambda (class-mapping)
		 (getf class-mapping :class-name))))

(defun compute-inverted-one-to-many (referenced-class-name
				     &optional class-mapping
				     &rest class-mappings)
  (when (not (null class-mapping))
    (destructuring-bind (&key class-name one-to-many-mappings
			      &allow-other-keys)
	class-mapping
      (reduce #'list*
	      (remove-if-not #'(lambda (one-to-many-mapping)
			        (eq referenced-class-name 
				    (reference-class-of one-to-many-mapping)))
			     one-to-many-mappings)
	      :key #'(lambda (one-to-many-mapping)
		       (list :one-to-many (slot-name-of one-to-many-mapping)
			     :reference-class-name class-name
			     :foreign-key (foreign-key-of one-to-many-mapping)
			     :serializer (getf one-to-many-mapping :serializer)
			     :deserializer (getf one-to-many-mapping :deserializer)))
	      :initial-value (apply #'compute-inverted-one-to-many
				    referenced-class-name
				    class-mappings)
	      :from-end t))))

(defun compute-superclass-mapping (&key class-name superclass-mappings
				     table-name primary-key
				     foreign-key properties
				     many-to-one-mappings
				     one-to-many-mappings)
  (let ((superclass-mappings
	 (apply #'compute-superclass-mappings
		superclass-mappings))
	(inverted-one-to-many
	 (apply #'compute-inverted-one-to-many
		class-name *class-mappings*)))
    (list :class-name class-name
	  :table-name table-name
	  :columns (remove-duplicates
		    (append primary-key
			    foreign-key
			    (reduce #'append superclass-mappings
				    :key #'foreign-key-of
				    :initial-value nil)
			    (reduce #'list* properties
				    :key #'column-of :from-end t
				    :initial-value nil)
			    (reduce #'append inverted-one-to-many
				    :key #'foreign-key-of
				    :initial-value nil))
		    :test #'string=)
	  :properties properties
	  :many-to-one-mappings many-to-one-mappings
	  :one-to-many-mappings one-to-many-mappings
	  :inverted-one-to-many inverted-one-to-many
	  :primary-key primary-key
	  :foreign-key foreign-key
	  :superclass-mappings superclass-mappings)))

(defun compute-superclass-mappings (&rest superclass-mappings)
  (mapcar #'(lambda (superclass-mapping)
	      (destructuring-bind (class-name &rest foreign-key)
		  superclass-mapping
		(list :reference-class-name class-name
		      :foreign-key foreign-key)))
	  superclass-mappings))

(defun compute-subclass-mappings (class-name &optional
					       (class-mappings
						*class-mappings*))
  (mapcar #'(lambda (class-mapping)
		(list :reference-class-name (class-name-of class-mapping)
		      :foreign-key (rest
				    (find class-name
					  (superclass-mappings-of class-mapping)
					  :key #'first))))
	  (remove-if-not #'(lambda (class-mapping)
			     (destructuring-bind
				   (&key superclass-mappings
					 &allow-other-keys)
				 class-mapping
			       (find class-name superclass-mappings
				     :key #'first)))
			 class-mappings)))

(defun compute-class-mapping (&key class-name table-name primary-key
				superclass-mappings properties
				many-to-one-mappings
				one-to-many-mappings root-class
				foreign-key)
  (let* ((inverted-one-to-many
	  (apply #'compute-inverted-one-to-many
		 class-name *class-mappings*))
	 (superclass-mappings
	  (apply #'compute-superclass-mappings
		 (remove root-class superclass-mappings
			 :key #'first))))
    (list* :class-name class-name
	   :table-name table-name
	   :columns (remove-duplicates
		     (append primary-key
			     foreign-key
			     (reduce #'append superclass-mappings
				     :key #'foreign-key-of
				     :initial-value nil)
			     (reduce #'list* properties
				     :key #'column-of :from-end t
				     :initial-value nil)
			     (reduce #'append inverted-one-to-many
				     :key #'foreign-key-of
				     :initial-value nil))
		     :test #'string=)
	   :primary-key primary-key
	   :properties properties
	   :many-to-one-mappings many-to-one-mappings
	   :one-to-many-mappings one-to-many-mappings
	   :inverted-one-to-many inverted-one-to-many
	   :superclass-mappings superclass-mappings
	   :subclass-mappings (compute-subclass-mappings class-name)
	   (when (not (null foreign-key))
	     (list :foreign-key foreign-key)))))

(defmacro define-schema (name params &rest class-mappings)
  (declare (ignore params))
  `(defun ,name ()
     ,(let ((*class-mappings*
	     (mapcar #'parse-class-mapping class-mappings)))
	   `(quote ,(mapcar #'(lambda (class-mapping)
				(apply #'compute-class-mapping class-mapping))
			    *class-mappings*)))))
