(in-package #:cl-db)

(defvar *table-index*)

(defun get-class-mapping (class-name
			  &optional (mapping-schema *mapping-schema*))
  (assoc class-name mapping-schema))

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun append-alias (alias &rest column-names)
  (mapcar #'(lambda (column-name)
	      (list alias column-name))
	  column-names))

(defun append-join (table-join &rest slot-mappings)
  (reduce #'(lambda (result mapping)
	      (destructuring-bind (slot-definition . mapping)
		  mapping
		(acons slot-definition
		       #'(lambda ()
			   (multiple-value-bind
				 (columns loader from-clause)
			       (funcall mapping)
			     (values columns loader
				     (list* table-join from-clause))))
		       result)))
	  slot-mappings :initial-value nil))

(defun get-slot-definition (class reader)
  (or
   (slot-definition-name
    (find-if #'(lambda (slot-definition)
		 (find (generic-function-name reader)
		       (slot-definition-readers slot-definition)))
	     (class-direct-slots class)))
   (error "Slot with reader ~a not found for class ~a"
	  reader (class-name class))))

(defun plan-properties (alias &rest properties)
  (reduce #'(lambda (result property)
	      (destructuring-bind
		    (slot-definition column-name column-type)
		  property
		(declare (ignore column-type))
		(let ((column
		       (concatenate 'string alias "." column-name)))
		  (acons slot-definition
			 #'(lambda ()
			     (values
			      (list alias column-name)
			      nil
			      #'(lambda (object &rest row)
				  (setf
				   (slot-value object 
					       (slot-definition-name slot-definition))
				   (rest (assoc column row))))))
			 result))))
	  properties :initial-value nil))

(defun plan-many-to-one (slot-definition class-name &rest foreign-key)
  (destructuring-bind (class-name &key table-name primary-key
				  properties one-to-many-mappings
				  many-to-one-mappings
				  superclass-mappings
				  subclass-mappings)
      (get-class-mapping class-name)
    (let* ((alias (make-alias))
	   (table-join
	    (list :left-join table-name alias
		  (mapcar #'append foreign-key
			  (apply #'append-alias alias primary-key)))))
      (plan-class-mapping class-name alias table-join primary-key
			  properties one-to-many-mappings
			  many-to-one-mappings superclass-mappings
			  subclass-mappings))))

(defun plan-many-to-one-mappings (alias &rest many-to-one-mappings)
  (reduce #'(lambda (result many-to-one-mapping)
	      (destructuring-bind (slot-definition
				   &key reference-class-name foreign-key)
		  many-to-one-mapping
		(acons slot-definition
		       #'(lambda ()
			   (apply #'plan-many-to-one
				  slot-definition reference-class-name
				  (apply #'append-alias
					 alias foreign-key)))
		       result)))
	  many-to-one-mappings :initial-value nil))

(defun plan-one-to-many (slot-definition class-name
			 root-primary-key foreign-key
			 serializer desirealizer)
  (destructuring-bind (class-name &key table-name primary-key
				  properties one-to-many-mappings
				  many-to-one-mappings
				  superclass-mappings
				  subclass-mappings)
      (get-class-mapping class-name)
    (let* ((alias (make-alias))
	   (table-join
	    (list :left-join table-name alias
		  (mapcar #'append root-primary-key
			  (apply #'append-alias alias foreign-key)))))
      (plan-class-mapping class-name alias table-join primary-key
			  properties one-to-many-mappings
			  many-to-one-mappings superclass-mappings
			  subclass-mappings))))

(defun plan-one-to-many-mappings (primary-key
				  &rest one-to-many-mappings)
  (reduce #'(lambda (result one-to-many-mapping)
	      (destructuring-bind (slot-definition
				   &key reference-class-name
				   foreign-key serializer deserializer)
		  one-to-many-mapping
		(acons slot-definition
		       #'(lambda ()
			   (plan-one-to-many slot-definition
					     reference-class-name
					     primary-key foreign-key
					     serializer deserializer))
		       result)))
	  one-to-many-mappings :initial-value nil))

(defun plan-superclass-slot-mappings (next-superclass-fn
				      rest-references
				      subclass-alias class-name
				      &key table-name primary-key
					foreign-key properties
					one-to-many-mappings
					many-to-one-mappings
					superclass-mappings)
  (let* ((alias
	  (make-alias))
	 (primary-key
	  (apply #'append-alias alias primary-key))
	 (foreign-key
	  (apply #'append-alias subclass-alias foreign-key)))
    (values
     #'(lambda (function)
	 (funcall function
		  (find-class class-name)
		  primary-key
		  (list :inner-join table-name alias
			(mapcar #'append primary-key foreign-key))
		  (apply #'plan-properties alias properties)
		  (apply #'plan-superclasses-slot-mappings
			 alias superclass-mappings)
		  next-superclass-fn))
     (append rest-references
	     (apply #'plan-many-to-one-mappings
		    alias many-to-one-mappings)
	     (apply #'plan-one-to-many-mappings
		    primary-key one-to-many-mappings)))))

(defun plan-superclasses-slot-mappings (subclass-alias
					superclass-mapping
					&rest superclass-mappings)
  (multiple-value-bind (next-superclass-fn rest-references)
      (apply #'plan-superclasses-slot-mappings
	     subclass-alias superclass-mappings)
    (apply #'plan-superclass-slot-mappings next-superclass-fn
	   rest-references subclass-alias superclass-mapping)))

(defun plan-class-mapping (class-name alias table-join primary-key
			   properties one-to-many-mappings
			   many-to-one-mappings superclass-mappings
			   subclass-mappings)
  (multiple-value-bind (superclass-continuation-fn superclass-references)
      (when (not (null superclass-mappings))
	(apply #'plan-superclasses-slot-mappings
	       alias superclass-mappings))
    (let ((class (find-class class-name))
	  (primary-key
	   (apply #'append-alias alias primary-key)))
      (multiple-value-bind (subclass-loader-fn subclass-references)
	  (apply #'plan-subclass-mappings
		 table-join
		 (apply #'append-alias alias primary-key)
		 subclass-mappings)
	(values #'(lambda (function)
		    (funcall function class primary-key table-join
			     (apply #'plan-properties alias properties)
			     superclass-continuation-fn
			     subclass-loader-fn))
		(append (apply #'plan-one-to-many-mappings
			       primary-key one-to-many-mappings)
			(apply #'plan-many-to-one-mappings
			       alias many-to-one-mappings)
			superclass-references)
		subclass-references)))))




;; path dispatch (next-property, next-superclass)?

(defun register-object (class primary-key object objects)
  (setf (gethash primary-key
		 (ensure-gethash class objects
				 (make-hash-table :test #'equal)))
	object))

(defun select-properties (&optional property &rest properties) ;; rename
  (when (not (null property))
    (multiple-value-bind (column loader)
	(funcall (rest property))
      (multiple-value-bind (columns rest-loader)
	  (apply #'select-properties properties)
	(values
	 (list* column columns)
	 #'(lambda (object)
	     (funcall loader object)
	     (funcall rest-loader object)))))))



(defun select-superclasses (class primary-key table-join
			    properties next-superclass-fn)
  (multiple-value-bind (columns from-clause superclass-loader)
      (funcall next-superclass-fn #'select-superclass)
    (multiple-value-bind (property-columns property-loader)
	(apply #'select-properties properties)
      (values
       (append primary-key property-columns columns)
       (list* table-join from-clause)
       #'(lambda (objects object)
	   (funcall superclass-loader object)
	   (funcall property-loader object)
	   (register-object class primary-key object objects))))))
    
(defun select-class (class primary-key table-join properties
		     superclass-continuations subclass-continuation)
  (multiple-value-bind (columns from-clause superclasses-loader-fn)
      (apply #'select-superclasses class primary-key table-join
	     properties superclass-continuations)
    (multiple-value-bind (subclasses-columns
			  subclasses-from-clause subclasses-loader-fn)
	(funcall subclass-continuation #'select-class)
      (values
       (append columns subclasses-columns)
       (append from-clause subclasses-from-clause)
       #'(lambda (objects &rest row)
	   (when (every #'(lambda (column-name)
			    (rest (assoc column-name row)))
			primary-key)
	     (funcall superclasses-loader-fn objects
		      (or (funcall subclasses-loader-fn objects row)
			  (allocate-instance class)))))))))

(defun plan-class-slots (class alias table-join primary-key
			 properties one-to-many-mappings
			 many-to-one-mappingsuperclass-mappings)


  (multiple-value-bind (rest-columns rest-from-clause rest-loaders)
      (plan-superclasses alias superclasss-mappings)
    (multiple-value-bind (columns from-clause loader)
	(funcall next-superclass-fn #'select-superclass)
      (values
       (append super-columns primary-key columns)
       (append super-from-clause from-clause)
       #'(lambda (objects object)
	   (funcall super-superclass-loader objects object)
	   (funcall superclass-loader objects object))))))






(defun plan-subclass-slots (class alias table-join primary-key
			    properties superclass-mappings)
  (

(defun plan-subclass-mapping (superclass-primary-key
			      &key class-name primary-key foreign-key
			      properties one-to-many-mappings
			      many-to-one-mappings superclass-mappings
			      subclass-mappings)
  (let* ((alias
	  (make-alias))
	 (foreign-key
	  (apply #'append-alias alias foreign-key))
	 (table-join
	  (list* :inner-join table-name alias
		 (mapcar #'append superclass-primary-key foreign-key))))
    (multiple-value-bind (references columns from-clause class-loader)
	(plan-subclass-mapping-slots class alias table-join primary-key
				     properties superclass-mappings)
      (multiple-value-bind (fetch-references columns from-clause loader)
	  (plan-class-selection references columns from-clause
				class-loader class alias
				subclass-mappings)
	(values references columns from-clause loader)))))

(defun plan-subclass-mappings (superclass-primary-key
			       &optional subclass-mapping
			       &rest subclass-mappings)
  (when (not (null subclass-mapping))
    (multiple-value-bind (rest-references rest-columns
			  rest-from-clause loaders)
	(apply #'plan-subclass-mappings
	       superclass-primary-key subclass-mappings)
      (multiple-value-bind (references columns from-clause loader)
	  (apply #'plan-subclass-mapping
		 superclass-primary-key subclass-mapping)
	(values
	 (append references rest-references)
	 (append columns rest-columns)
	 (append from-clause rest-from-clause)
	 (list* loader loaders))))))

(defun plan-class-selection (refernces columns from-clause
			     superclass-loader-fn class primary-key
			     subclass-mappings)
  (multiple-value-bind (subclass-references subclass-columns
			subclass-from-clause subclasses-loader-fn)
      (plan-subclass-mappings primary-key subclass-mappings)
    (values (append references subclass-refernces)
	    (append columns subclass-columns)
	    (append from-clause subclass-from-clause)
	    #'(lambda (objects &rest row)
		(when (every #'(lambda (column-name)
				 (rest (assoc column-name row)))
			     primary-key)
		  (funcall superclasses-loader-fn objects
			   (or (some #'(lambda (loader)
					 (funcall loader objects row))
				     subclass-loaders)
			       (allocate-instance class))))))))

(defun plan-root-class-mapping (alias class-name
				&key table-name primary-key properties
				  one-to-many-mappings
				  many-to-one-mappings
				  superclass-mappings
				  subclass-mappings)
  (let ((class (find-class class-name))
	(table-join (list table-name alias)))
    (multiple-value-bind (properties join-references columns
			  from-clause superclass-loader)
	(plan-class-slots class alias table-join primary-key
			  properties one-to-many-mappings
			  many-to-one-mappings superclass-mappings)
      (multiple-value-bind (fetch-references columns from-clause
			    loader)
	  (plan-class-selection join-references columns from-clause
				superclass-loader class alias
				subclass-mappings)
	(values
	 #'(lambda (&optional (property-reader nil name-present-p))
	     (if name-present-p
		 (rest
		  (assoc (get-slot-definition class property-reader)
			 properties))
		 #'(lambda ()
		     (values columns from-clause loader))))
	 join-references
	 fetch-references)))))
  
(defun make-join-plan (mapping-schema class-name &rest class-names)
  (multiple-value-bind (rest-properties-and-loaders
			rest-join-references rest-fetch-references)
      (when (not (null class-names))
	(apply #'make-join-plan mapping-schema class-names))
    (multiple-value-bind (properties-and-loaders-fn
			  join-references-fn fetch-references-fn)
	(apply #'plan-root-class-mapping
	       (make-alias)
	       (get-class-mapping class-name mapping-schema))
      (values
       (list* properties-and-loaders-fn rest-properties-and-loaders)
       (list* join-references-fn rest-join-references)
       (list* fetch-references-fn rest-fetch-references)))))

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

;; 1) Implement many roots
;; 2) Implement loaders

(defun property (reader entity)
  (funcall entity reader))

;;(defun compute-select-list (select-list-element
;;			    &rest select-list-elements)
;;  (if (not (null select-list-element))
;;      (apply #'compute-list root
;;	     (apply #'compute-select-list select-list-elements))
;;      root))

(defun db-read (roots &key ;;select-list
			where order-by having offset limit fetch-also
			singlep transform
			(mapping-schema *mapping-schema*))
  (declare (ignore where order-by having limit offset
		   transform fetch-also singlep))
  (let ((*table-index* 0))
    (multiple-value-bind (properties-and-loaders
			  join-references fetch-references)
	(if (not (listp roots))
	    (make-join-plan mapping-schema roots)
	    (apply #'make-join-plan mapping-schema roots))
      (values
       (funcall properties-and-loaders)
       :join-references join-references
       :fetch-references fetch-references))))
