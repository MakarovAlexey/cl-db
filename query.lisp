(in-package #:cl-db)

(defvar *table-index*)

(defun get-class-mapping (class-name
			  &optional (mapping-schema *mapping-schema*))
  (assoc class-name mapping-schema))

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun append-alias (alias column-names)
  (mapcar #'(lambda (column-name)
	      (concatenate 'string alias "." column-name))
	  column-names))

(defun append-join (table-join slot-mappings)
  (reduce #'(lambda (result mapping)
	      (destructuring-bind (slot-name . mapping) mapping
		(acons slot-name
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

(defun plan-properties (alias &optional property &rest properties)
  (when (not (null property))
    (multiple-value-bind (planned-properties columns loaders)
	(apply #'plan-properties alias properties)
      (destructuring-bind (slot-name column-name column-type)
	  property
	(declare (ignore column-type))
	(let* ((column (concatenate 'string alias "." column-name))
	       (loader #'(lambda (object row)
			   (setf
			    (slot-value object slot-name)
			    (rest
			     (assoc column row :test #'string=))))))
	  (values (acons slot-name
			 #'(lambda ()
			     (values column nil loader))
			 planned-properties)
		  (list* column columns)
		  (list* loader loaders)))))))

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
			  (append-alias alias primary-key)))))
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
				  (append-alias alias foreign-key)))
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
   
(defun plan-superclass (subclass-alias &key class-name table-name
					 primary-key foreign-key
					 properties
					 one-to-many-mappings
					 many-to-one-mappings
					 superclass-mappings)
  (let* ((class
	  (find-class class-name))
	 (alias
	  (make-alias))
	 (foreign-key
	  (append-alias subclass-alias foreign-key))
	 (primary-key
	  (append-alias alias primary-key))
	 (table-join
	  (list* :inner-join table-name alias
		 (mapcar #'list primary-key foreign-key))))
    (plan-class-slots class alias table-join primary-key
		      properties one-to-many-mappings
		      many-to-one-mappings superclass-mappings)))

(defun plan-superclasses (alias &optional superclass-mapping
			  &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (properties references
			  columns from-clause loaders)
	(apply #'plan-superclass alias superclass-mapping)
      (multiple-value-bind (rest-properties rest-references
			    rest-columns rest-from-clause rest-loaders)
	  (apply #'plan-superclasses alias superclass-mappings)
	(values 
	 (append properties rest-properties)
	 (append references rest-references)
	 (append columns rest-columns)
	 (append from-clause rest-from-clause)
	 (append loaders rest-loaders))))))

(defun register-object (class primary-key object objects)
  (setf (gethash primary-key
		 (ensure-gethash class objects
				 (make-hash-table :test #'equal)))
	object))

(defun plan-class-slots (class alias table-join primary-key
			 properties one-to-many-mappings
			 many-to-one-mappings superclass-mappings)
  (multiple-value-bind (properties columns property-loaders)
      (apply #'plan-properties alias properties)
    (multiple-value-bind (rest-properties references
			  rest-columns from-clause loaders)
	(apply #'plan-superclasses alias superclass-mappings)
      (values
       (append-join table-join (append properties rest-properties))
       (append-join table-join
		    (append (apply #'plan-one-to-many-mappings
				   primary-key one-to-many-mappings)
			    (apply #'plan-many-to-one-mappings
				   alias many-to-one-mappings)
			    references))
       (append primary-key columns rest-columns)
       (list* table-join from-clause)
       (list* #'(lambda (objects object row)
		  (dolist (loader property-loaders)
		    (funcall loader object row))
		  (register-object class primary-key object objects))
	      loaders)))))

(defun plan-subclass-mapping (superclass-primary-key class-name
			      &key primary-key table-name foreign-key
				properties one-to-many-mappings
				many-to-one-mappings
				superclass-mappings subclass-mappings)
  (let* ((class (find-class class-name))
	 (alias (make-alias))
	 (foreign-key (append-alias alias foreign-key))
	 (table-join
	  (list* :left-join table-name alias
		 (mapcar #'list superclass-primary-key foreign-key)))
	 (primary-key (append-alias alias primary-key)))
    (multiple-value-bind (properties references columns
			  from-clause superclass-loaders)
	(plan-class-slots class alias table-join primary-key
			  properties one-to-many-mappings
			  many-to-one-mappings superclass-mappings)
      (declare (ignore properties))
      (plan-class-selection references columns from-clause
			    superclass-loaders class alias
			    subclass-mappings))))

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

(defun plan-class-selection (references columns from-clause
			     superclass-loaders class primary-key
			     subclass-mappings)
  (multiple-value-bind (subclass-references subclass-columns
			subclass-from-clause subclass-loaders)
      (apply #'plan-subclass-mappings primary-key subclass-mappings)
    (values (append references subclass-references)
	    (append columns subclass-columns)
	    (append from-clause subclass-from-clause)
	    #'(lambda (objects &rest row)
		(when (every #'(lambda (column-name)
				 (rest (assoc column-name row)))
			     primary-key)
		  (reduce #'(lambda (object loader)
			      (funcall loader objects object row))
			  superclass-loaders
			  :initial-value
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
	(table-join (list table-name alias))
	(primary-key (append-alias alias primary-key)))
    (multiple-value-bind (properties join-references columns
			  from-clause superclass-loader)
	(plan-class-slots class alias table-join primary-key
			  properties one-to-many-mappings
			  many-to-one-mappings superclass-mappings)
      (multiple-value-bind (fetch-references columns
			    from-clause loader)
	  (plan-class-selection join-references columns from-clause
				superclass-loader class primary-key
				subclass-mappings)
	(values
	 #'(lambda (&optional (property-reader nil name-present-p))
	     (if name-present-p
		 (funcall
		  (rest
		   (assoc (get-slot-definition class property-reader)
			  properties)))
		 (values columns from-clause loader)))
	 join-references
	 fetch-references)))))
  
(defun make-join-plan (mapping-schema class-name &rest class-names)
  (multiple-value-bind (selectors rest-join-references
		        rest-fetch-references)
      (when (not (null class-names))
	(apply #'make-join-plan mapping-schema class-names))
    (multiple-value-bind (selector join-references fetch-references)
	(apply #'plan-root-class-mapping (make-alias)
	       (get-class-mapping class-name mapping-schema))
      (values
       (list* selector selectors)
       (list* join-references rest-join-references)
       (list* fetch-references rest-fetch-references)))))

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
    (multiple-value-bind (selectors join-references fetch-references)
	(if (not (listp roots))
	    (make-join-plan mapping-schema roots)
	    (apply #'make-join-plan mapping-schema roots))
      (values selectors join-references fetch-references))))
