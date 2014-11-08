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
			      #'(lambda (object &rest row)
				  (setf
				   (slot-value object 
					       (slot-definition-name slot-definition))
				   (rest (assoc column row))))))
			 result))))
	  properties :initial-value nil))

(defun plan-properties (alias &rest properties)
  #'(lambda (&optional slot-name)
      (destructuring-bind (slot-name column-name column-type)
	  (rest (assoc slot-name properties))
	(declare (ignore column-type))
	(let ((column
	       (concatenate 'string alias "." column-name)))
	  (values
	   (list alias column-name)
	   #'(lambda (object &rest row)
	       (setf
		(slot-value object slot-name)
		(rest (assoc column row) result))))))))

(defun plan-many-to-one (slot-definition class-name &rest foreign-key)
  (destructuring-bind (class-name &key table-name primary-key
				  properties one-to-many-mappings
				  many-to-one-mappings
				  superclass-mappings
				  subclass-mappings)
      (get-class-mapping class-name)
    (declare (ignore class-name))
    (let* ((alias (make-alias))
	   (table-join
	    (list :left-join table-name alias
		  (mapcar #'append foreign-key
			  (apply #'append-alias alias primary-key)))))
      (plan-class-mapping alias table-join primary-key properties
			  one-to-many-mappings many-to-one-mappings
			  superclass-mappings subclass-mappings))))

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
      (plan-class-mapping alias table-join primary-key properties
			  one-to-many-mappings many-to-one-mappings
			  superclass-mappings subclass-mappings))))

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

(defun plan-superclass-slot-mappings (subclass-alias class-name
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
    (apply #'plan-superclasses-slot-mappings
	   (apply #'plan-properties alias properties)
	   (append
	    (apply #'plan-many-to-one-mappings
		   alias many-to-one-mappings)
	    (apply #'plan-one-to-many-mappings
		   primary-key one-to-many-mappings))
	   (list :inner-join table-name alias
		 (mapcar #'append primary-key foreign-key))
	   alias superclass-mappings)))
;; (append-join subclass-table-join 
(defun plan-superclasses-slot-mappings (properties references
					subclass-table-join subclass-alias
					&optional superclass-mapping
					&rest superclass-mappings)
  (multiple-value-bind (rest-properties rest-references)
      (when (not (null superclass-mapping))
	(multiple-value-bind (properties references)
	    (apply #'plan-superclass-slot-mappings
		   subclass-alias superclass-mapping)
	  (apply #'plan-superclasses-slot-mappings properties
		 references subclass-table-join subclass-alias
		 superclass-mappings)))
    (values
     #'(lambda (&optional slot-name)
	 (if (not (null slot-name))
	     (or
	      (funcall properties slot-name)
	      (funcall rest-properties slot-name))
	     

	       
     (append (apply #'append-join subclass-table-join properties)
	     rest-properties)
     (append (apply #'append-join subclass-table-join references)
	     rest-references))))

(defun plan-class-mapping (class-name alias table-join primary-key
			   properties one-to-many-mappings
			   many-to-one-mappings superclass-mappings
			   subclass-mappings)
  (let ((class (find-class class-name))
	(primary-key
	 (apply #'append-alias alias primary-key)))
    (multiple-value-bind (properties references)
	(apply #'plan-superclasses-slot-mappings
	       (apply #'plan-properties alias properties)
	       (append (apply #'plan-one-to-many-mappings
			      primary-key one-to-many-mappings)
		       (apply #'plan-many-to-one-mappings
			      alias many-to-one-mappings))
	       table-join alias superclass-mappings)
      (multiple-value-bind (all-references)
	  (apply #'plan-subclass-mappings references
		 table-join
		 (apply #'append-alias alias primary-key)
		 subclass-mappings)
	(values
	 #'(lambda (&optional (property-reader nil name-present-p))
	     (if name-present-p
		 (rest
		  (assoc (get-slot-definition class property-reader)
			 properties))
		 (select-class-mapping class-name primary-key
				       properties)))
	 #'(lambda (reference-reader)
	     (rest
	      (assoc (get-slot-definition class reference-reader)
		     references)))
	 #'(lambda (reference-reader)
	     (rest
	      (assoc (get-slot-definition class reference-reader)
		     all-references))))))))

(defun plan-subclass-mapping (superclass-primary-key class-name
			      &key table-name primary-key
				foreign-key properties
				one-to-many-mappings
				many-to-one-mappings
				superclass-mappings
				subclass-mappings)
  (let* ((alias
	  (make-alias))
	 (foreign-key
	  (apply #'append-alias alias foreign-key))
	 (table-join
	  (list* :inner-join table-name alias
		 (mapcar #'append superclass-primary-key foreign-key))))
    (plan-class-mapping class-name alias table-join primary-key
			properties one-to-many-mappings
			many-to-one-mappings superclass-mappings
			subclass-mappings)))

(defun plan-subclass-mappings (references table-join
			       superclass-primary-key
			       &optional subclass-mapping
			       &rest subclass-mappings)
  (multiple-value-bind (subclass-references)
      (when (not (null subclass-mapping))
	(multiple-value-bind (subclass-references)
	    (apply #'plan-subclass-mapping
		   superclass-primary-key
		   subclass-mapping)
	  (apply #'plan-subclass-mappings
		 subclass-references
		 table-join
		 superclass-primary-key
		 subclass-mappings)))
    (append references
	    (apply #'append-join table-join subclass-references))))

(defun plan-root-class-mapping (alias class-name
				&key table-name primary-key properties
				  one-to-many-mappings many-to-one-mappings
				  superclass-mappings subclass-mappings)
  (let ((table-join (list table-name alias)))
    (plan-class-mapping class-name alias table-join primary-key
			properties one-to-many-mappings
			many-to-one-mappings superclass-mappings
			subclass-mappings)))

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

(defun compute-select-list (select-list-element
			    &rest select-list-elements)
  (if (not (null select-list-element))
      (apply #'compute-list root
	     (apply #'compute-select-list select-list-elements))
      root))

(defun db-read (roots &key select-list
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
       (apply (or select-list #'compute-select-list)
	      properties-and-loaders)
       :join-references join-references
       :fetch-references fetch-references))))
