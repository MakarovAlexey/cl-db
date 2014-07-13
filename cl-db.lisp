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

(defun parse-class-mapping (class-mappings)
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
	    :superclasses superclasses
	    :value-mappings (mapcar #'rest
				    (remove-if-not
				     #'(lambda (mapping)
					 (eq (first mapping) :property))
				     slot-mappings))
	    :many-to-one (mapcar #'rest
				 (remove-if-not
				  #'(lambda (mapping)
				      (eq (first mapping) :many-to-one))
				  slot-mappings))
	    :one-to-many (mapcar #'rest
				 (remove-if-not
				  #'(lambda (mapping)
				      (eq (first mapping) :one-to-many))
				  slot-mappings))))))

(defun find-class-mapping (class-name &optional (class-mappings
						 *class-mappings*))
  (find class-name class-mappings
	:key #'(lambda (class-mapping)
		 (getf :class-name class-mapping))))

(defun compute-superclass-mappings (class-name &rest foreign-key)
  (let ((superclass-mappings
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'compute-superclass-mapping superclass-mapping))
		 superclasses))
	(list :class-name class-name
	      :table-name table-name
	      :primary-key primary-key
	      :superclasses superclass-mappings
  
	      

(defun compute-class-mapping (&key class-name table-name primary-key
				superclasses value-mappings
				many-to-one one-to-many)
  (let ((superclass-mappings
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'compute-superclass-mapping superclass-mapping))
		 superclasses))
;;	(mapping-precedence-list
;;	 (apply #'compute-mapping-presenece-list superclass-mappings))
	)
    (list :class-name class-name
	  :table-name table-name
	  :primary-key primary-key
	  :superclasses superclass-mappings
;;	:mapping-precedence-list mapping-precedence-list
;	:subclasses (apply #'compute-subclass-mappings
;			   class-name mapping-precedence-list)
;;	:value-mapping (apply #'
	  )))

(defmacro define-schema (name params &rest class-mappings)
  (declare (ignore params))
  `(let ((*class-mappings*
	  (quote ,(apply #'parse-mappings class-mappings))))
     (mapcar #'(lambda (class-mapping)
		 (apply #'compute-class-mapping class-mapping))
	     *class-mappings*)))
