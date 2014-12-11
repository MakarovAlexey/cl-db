(in-package #:cl-db)

(defvar *table-index*)

(defun get-class-mapping (class-name
			  &optional (mapping-schema *mapping-schema*))
  (or
   (assoc class-name mapping-schema)
   (error "class mapping for class ~a not found" class-name)))

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun append-alias (alias column-names)
  (mapcar #'(lambda (column-name)
	      (list alias column-name))
	  column-names))

(defun plan-primary-key (pk-column &rest primary-key)
  (multiple-value-bind (primary-key-columns primary-key-loader)
      (when (not (null primary-key))
	(apply #'plan-primary-key primary-key))
    (let ((column-alias (make-alias "column")))
      (values
       (list* #'(lambda ()
		  (values pk-column column-alias))
	      primary-key-columns)
       nil
       (list* #'(lambda (row)
		  (rest
		   (assoc column-alias row :test #'string=)))
	      primary-key-loader)))))

(defun get-slot-name (class reader)
  (let ((slot-definition
	 (find-if #'(lambda (slot-definition)
		      (find (generic-function-name reader)
			    (slot-definition-readers slot-definition)))
		  (class-direct-slots class))))
    (when (null slot-definition)
      (error "Slot with reader ~a not found for class ~a"
	     reader (class-name class)))
    (slot-definition-name slot-definition)))

(defun plan-properties (alias &optional property &rest properties)
  (when (not (null property))
    (multiple-value-bind (planned-properties columns loaders)
	(apply #'plan-properties alias properties)
      (destructuring-bind (slot-name column-name column-type)
	  property
	(declare (ignore column-type))
	(let* ((column-alias (make-alias "column"))
	       (column #'(lambda ()
			   (values (list alias column-name)
				   column-alias)))
	       (loader #'(lambda (object row)
			   (setf
			    (slot-value object slot-name)
			    (rest
			     (assoc column-alias row
				    :test #'string=))))))
	  (values (acons slot-name
			 #'(lambda ()
			     (values column nil loader))
			 planned-properties)
		  (list* column columns)
		  (list* loader loaders)))))))

;; join - as root
;; fetch - slot load
(defun plan-many-to-one (mode slot-name foreign-key class-name
			 &key table-name primary-key properties
			   one-to-many-mappings many-to-one-mappings
			   superclass-mappings subclass-mappings)
  (declare (ignore slot-name mode))
  (let* ((alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'list foreign-key
			(append-alias alias primary-key)))))
    (plan-class-mapping alias class-name table-join primary-key
			properties one-to-many-mappings
			many-to-one-mappings superclass-mappings
			subclass-mappings)))

(defun plan-many-to-one-mappings (alias &rest many-to-one-mappings)
  (reduce #'(lambda (result many-to-one-mapping)
	      (destructuring-bind (slot-name &key reference-class-name
					     foreign-key)
		  many-to-one-mapping
		(acons slot-name
		       #'(lambda (mode)
			   (apply #'plan-many-to-one mode slot-name
				  (append-alias alias foreign-key)
				  (get-class-mapping reference-class-name)))
		       result)))
	  many-to-one-mappings :initial-value nil))

(defun plan-one-to-many (mode slot-name root-primary-key foreign-key
			 serializer deserializer class-name
			 &key table-name primary-key properties
			   one-to-many-mappings many-to-one-mappings
			   superclass-mappings subclass-mappings)
  (declare (ignore slot-name serializer deserializer mode))
  (let* ((alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'list root-primary-key
			(append-alias alias foreign-key)))))
    (plan-class-mapping alias class-name table-join primary-key
			properties one-to-many-mappings
			many-to-one-mappings superclass-mappings
			subclass-mappings)))

(defun plan-one-to-many-mappings (primary-key
				  &rest one-to-many-mappings)
  (reduce #'(lambda (result one-to-many-mapping)
	      (destructuring-bind (slot-name &key reference-class-name
					     foreign-key
					     serializer deserializer)
		  one-to-many-mapping
		(acons slot-name
		       #'(lambda (mode)
			   (apply #'plan-one-to-many mode slot-name
				  primary-key foreign-key
				  serializer deserializer
				  (get-class-mapping reference-class-name)))
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
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-primary-key primary-key)
      (plan-class-slots class alias table-join primary-key
			 primary-key-columns primary-key-loader
			 properties one-to-many-mappings
			 many-to-one-mappings superclass-mappings))))

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

(defun append-join (table-join selector)
  #'(lambda (&rest args)
      (multiple-value-bind (columns from-clause loader)
	  (apply selector args)
	(values columns (list* table-join from-clause) loader))))

(defun reference-append-join (table-join reference-fn)
  #'(lambda (&rest args)
      (multiple-value-bind (selector references)
	  (apply reference-fn args)
	(values
	 (append-join table-join selector)
	 (reference-append-join table-join references)))))

(defun plan-class-slots (class alias table-join primary-key
			 primary-key-columns primary-key-loaders
			 properties one-to-many-mappings
			 many-to-one-mappings superclass-mappings)
  (multiple-value-bind (properties columns property-loaders)
      (apply #'plan-properties alias properties)
    (multiple-value-bind (rest-properties references
			  rest-columns from-clause loaders)
	(apply #'plan-superclasses alias superclass-mappings)
      (values
       (reduce #'(lambda (result property)
		   (destructuring-bind (slot-name . selector) property
		     (acons slot-name
			    (append-join table-join selector)
			    result)))
	       (append properties rest-properties)
	       :initial-value nil)
       (reduce #'(lambda (result reference)
		   (destructuring-bind (slot-name . mapping) reference
		     (acons slot-name
			    (reference-append-join table-join mapping)
			    result)))
	       (append (apply #'plan-one-to-many-mappings
			      primary-key one-to-many-mappings)
		       (apply #'plan-many-to-one-mappings
			      alias many-to-one-mappings)
		       references)
	       :initial-value nil)
       (append primary-key-columns columns rest-columns)
       (list* table-join from-clause)
       (list* #'(lambda (objects object row)
		  (dolist (loader property-loaders)
		    (funcall loader object row))
		  (register-object class
				   (mapcar #'(lambda (loader)
					       (funcall loader row))
					    primary-key-loaders)
				   object objects))
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
    (multiple-value-bind (primary-key-columns primary-key-loaders)
	(apply #'plan-primary-key primary-key)
      (multiple-value-bind (properties references columns
			    from-clause superclass-loaders)
	  (plan-class-slots class alias table-join primary-key
			    primary-key-columns primary-key-loaders
			    properties one-to-many-mappings
			    many-to-one-mappings superclass-mappings)
	(declare (ignore properties))
	(plan-class-selection references columns from-clause
			      superclass-loaders class primary-key
			      primary-key-loaders
			      subclass-mappings)))))

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
			     primary-key-loaders subclass-mappings)
  (multiple-value-bind (subclass-references subclass-columns
			subclass-from-clause subclass-loaders)
      (apply #'plan-subclass-mappings primary-key subclass-mappings)
    (values (append references subclass-references)
	    (append columns subclass-columns)
	    (append from-clause subclass-from-clause)
	    #'(lambda (objects row)
		(when (every #'(lambda (loader)
				 (funcall loader row))
			     primary-key-loaders)
		  (reduce #'(lambda (object loader)
			      (funcall loader objects object row))
			  superclass-loaders
			  :initial-value
			  (or (some #'(lambda (loader)
					(funcall loader objects row))
				    subclass-loaders)
			      (allocate-instance class))))))))

(defun plan-class-mapping (alias class-name table-join primary-key
			   properties one-to-many-mappings
			   many-to-one-mappings superclass-mappings
			   subclass-mappings)
  (let ((class (find-class class-name))
	(primary-key (append-alias alias primary-key)))
    (multiple-value-bind (primary-key-columns primary-key-loaders)
	(apply #'plan-primary-key primary-key)
      (multiple-value-bind (properties join-references columns
			    from-clause superclass-loader)
	  (plan-class-slots class alias table-join primary-key
			    primary-key-columns primary-key-loaders
			    properties one-to-many-mappings
			    many-to-one-mappings superclass-mappings)
	(multiple-value-bind (fetch-references columns
			      from-clause loader)
	    (plan-class-selection join-references columns from-clause
				  superclass-loader class primary-key
				  primary-key-loaders subclass-mappings)
	  (values
	   #'(lambda (&optional (property-reader nil name-present-p))
	       (if name-present-p
		   (funcall
		    (rest
		     (assoc (get-slot-name class property-reader)
			    properties)))
		   (values columns from-clause loader)))
	   #'(lambda (reader mode)
	       (funcall (rest
			 (assoc (get-slot-name class reader)
				(if (eq mode 'join)
				    join-references
				    fetch-references)))
			mode))))))))

(defun plan-root-class-mapping (alias class-name
				&key table-name primary-key properties
				  one-to-many-mappings
				  many-to-one-mappings
				  superclass-mappings
				  subclass-mappings)
  (plan-class-mapping alias class-name (list table-name alias)
		      primary-key properties one-to-many-mappings
		      many-to-one-mappings superclass-mappings
		      subclass-mappings))
  
(defun make-join-plan (mapping-schema class-name &rest class-names)
  (multiple-value-bind (selectors rest-references)
      (when (not (null class-names))
	(apply #'make-join-plan mapping-schema class-names))
    (multiple-value-bind (selector references)
	(apply #'plan-root-class-mapping (make-alias)
	       (get-class-mapping class-name mapping-schema))
      (values
       (list* selector selectors)
       (list* references rest-references)))))

(defun property (reader entity)
  (funcall entity reader))

;;(defun compute-select-list (select-list-element
;;			    &rest select-list-elements)
;;  (if (not (null select-list-element))
;;      (apply #'compute-list root
;;	     (apply #'compute-select-list select-list-elements))
;;      root))

(defun join (references accessor alias &optional join)
  (multiple-value-bind (selector references)
      (funcall references accessor 'join)
    (list* alias selector
	   (when (not (null join))
	     (funcall join references)))))

(defun fetch (references accessor &optional fetch)
  (multiple-value-bind (selector references)
      (funcall references accessor 'fetch)
    (list* selector
	   (when (not (null fetch))
	     (funcall fetch references)))))
;; stubfunction, implement query creation.
;; implement GROUP BY clause
(defun make-query (select-list where-clause order-by-clause
		   having-clause limit offset)
  (lambda (&optional table-alias column-name)
    (if (and table-alias column-name)
	(list table-alias column-name)
	(values select-list where-clause order-by-clause having-clause
		limit offset))))

;; implemnt column name and table alias search
(defun make-subquery (query)
  (lambda (&optional table-alias column-name)
    query
    nil))

(defun compute-clause (args clause &optional default)
  (if (not (null clause))
      (multiple-value-list
       (apply clause args))
      default))

(defun db-read (roots &key join select where order-by having
			offset limit singlep transform fetch
			(mapping-schema *mapping-schema*))
  (declare (ignore transform singlep))
  (let ((*table-index* 0)
	(*mapping-schema* mapping-schema))
    (multiple-value-bind (selectors references)
	(if (not (listp roots))
	    (make-join-plan mapping-schema roots)
	    (apply #'make-join-plan mapping-schema roots))
      (let* ((joined-references
	      (list* selectors (compute-clause references join)))
	     (select-list
	      (compute-clause joined-references join roots))
	     (fetched-references
	      (compute-clause select-list fetch))
	     (query
	      (make-query select-list
			  (compute-clause joined-references where)
			  (compute-clause select-list order-by)
			  (when (not (null order-by))
			    (multiple-value-list
			     (apply order-by select-list)))
			  (multiple-value-list
			   (when (not (null having))
			     (apply having select-list)))
			  limit
			  offset)))
	     (reduce (lambda (query fetched-reference)
			 (funcall fetched-reference query))
		     fetched-references
		     :initial-value
		     (if (not (null (and fetch (or limit offset))))
			 (make-subquery query)
			 query))))))
