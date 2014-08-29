;;; cl-db.lisp

(in-package #:cl-db)

(defvar *class-mappings*)

(defun parse-slot-mappings (&rest slot-mappings)
  (loop for slot-mapping in slot-mappings collect
       (destructuring-bind
	     (slot-name (mapping-type &rest params)
			&optional deserializer serializer)
	   slot-mapping
	 (case mapping-type
	   (:property
	    (list :property
		  (list :slot-name slot-name
			:columns params
			:serializer serializer
			:deserializer deserializer)))
	   (:many-to-one
	    (list :many-to-one
		  (list :slot-name slot-name
			:reference-class-name (first params)
			:foreign-key (rest params))))
	   (:one-to-many
	    (list :one-to-many
		  (list :slot-name slot-name
			:reference-class-name (first params)
			:foreign-key (rest params)
			:serializer serializer
			:deserializer deserializer)))))))

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

(defun compute-superclass-mapping (&key class-name superclass-mappings
				     table-name primary-key
				     foreign-key property-mappings
				     many-to-one-mappings
				     one-to-many-mappings)
  (list :class-name class-name
	:table-name table-name
	:property-mappings property-mappings
	:many-to-one-mappings many-to-one-mappings
	:one-to-many-mappings one-to-many-mappings
	:primary-key primary-key
	:foreign-key foreign-key
	:superclass-mappings (apply #'compute-superclass-mappings
				    superclass-mappings)))

(defun compute-superclass-mappings (&rest superclass-mappings)
  (mapcar #'(lambda (superclass-mapping)
	      (destructuring-bind (class-name &rest foreign-key)
		  superclass-mapping
		(apply #'compute-superclass-mapping
		       :foreign-key foreign-key
		       (find-class-mapping class-name))))
	  superclass-mappings))

(defun compute-subclass-mappings (class-name &optional
					       (class-mappings
						*class-mappings*))
  (mapcar #'(lambda (class-mapping)
	      (destructuring-bind
		    (&key superclass-mappings &allow-other-keys)
		  class-mapping
		(destructuring-bind
		      (superclass-name &rest foreign-key)
		    (find class-name superclass-mappings :key #'first)
		  (apply #'compute-class-mapping
			 :root-class superclass-name
			 :foreign-key foreign-key
			 class-mapping))))
	  (remove-if-not #'(lambda (class-mapping)
			     (destructuring-bind
				   (&key superclass-mappings
					 &allow-other-keys)
				 class-mapping
			       (find class-name superclass-mappings
				     :key #'first)))
			 class-mappings)))

(defun compute-class-mapping (&key class-name table-name primary-key
				superclass-mappings property-mappings
				many-to-one-mappings
				one-to-many-mappings root-class
				foreign-key)
  (list* :class-name class-name
	 :table-name table-name
	 :primary-key primary-key
	 :property-mappings property-mappings
	 :many-to-one-mappings many-to-one-mappings
	 :one-to-many-mappings one-to-many-mappings
	 :superclass-mappings (apply #'compute-superclass-mappings
				     (remove root-class
					     superclass-mappings
					     :key #'first))
	 :subclass-mappings (compute-subclass-mappings class-name)
	 (when (not (null foreign-key))
	   (list :foreign-key foreign-key))))

(defmacro define-schema (name params &rest class-mappings)
  (declare (ignore params))
  `(defun ,name ()
     ,(let ((*class-mappings*
	     (mapcar #'parse-class-mapping class-mappings)))
	   `(quote ,(mapcar #'(lambda (class-mapping)
				(apply #'compute-class-mapping class-mapping))
			    *class-mappings*)))))
