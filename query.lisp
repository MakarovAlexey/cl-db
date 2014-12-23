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

(defun plan-column (table-alias column-name)
  (let ((alias (make-alias column-name)))
    (values #'(lambda ()
		(values column-name alias table-alias))
	    #'(lambda (row)
		(rest
		 (assoc alias row :test #'string=))))))

(defun plan-key (table-alias column &rest columns)
  (multiple-value-bind (key-columns key-loader)
      (when (not (null columns))
	(apply #'plan-key table-alias columns))
    (multiple-value-bind (key-column column-loader)
	(plan-column table-alias column)
      (values
       (list* key-column key-columns)
       nil
       #'(lambda (row)
	   (list*
	    (funcall column-loader row)
	    (when (not (null key-loader))
	      (funcall key-loader row))))))))

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
	(multiple-value-bind (column column-loader)
	    (plan-column alias column-name)
	  (let ((loader #'(lambda (object row)
			    (setf
			     (slot-value object slot-name)
			     (funcall column-loader row)))))
	    (values
	     (acons slot-name
		    #'(lambda ()
			(values column nil loader))
		    planned-properties)
	     (list* column columns)
	     (list* loader loaders))))))))

(defun fetch-properties (alias join-path
			 &optional property &rest properties)
  (when (not (null property))
    (multiple-value-bind (planned-properties columns loaders)
	(apply #'plan-properties alias properties)
      (destructuring-bind (slot-name column-name column-type)
	  property
	(declare (ignore column-type))
	(multiple-value-bind (column column-loader)
	    (plan-column alias column-name)
	  (let ((loader #'(lambda (object row)
			    (setf
			     (slot-value object slot-name)
			     (funcall column-loader row)))))
	    (values
	     (acons slot-name
		    #'(lambda ()
			(values column nil loader))
		    planned-properties)
	     (list* column columns)
	     (list* loader loaders))))))))

(defun plan-many-to-one-join (alias foreign-key class-name
			      &key table-name primary-key properties
				one-to-many-mappings
				many-to-one-mappings
				superclass-mappings subclass-mappings)
  (let* ((foreign-key
	  (append-alias alias foreign-key))
	 (alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'list foreign-key
			(append-alias alias primary-key)))))
    (join-class alias class-name table-join primary-key properties
		one-to-many-mappings many-to-one-mappings
		superclass-mappings subclass-mappings)))

(defun plan-many-to-one-mappings (alias &optional many-to-one-mapping
				  &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (multiple-value-bind (join-references fetch-references columns)
	(apply #'plan-many-to-one-mappings alias many-to-one-mappings)
      (destructuring-bind (slot-name
			   &key reference-class-name foreign-key)
	  many-to-one-mapping
	(multiple-value-bind (foreign-key-columns foreign-key-loader)
	    (plan-key alias foreign-key)
	  (values
	   (acons slot-name
		  #'(lambda ()
		      (apply #'plan-many-to-one-join
			     alias foreign-key
			     (get-class-mapping reference-class-name)))
		  join-references)
	   (acons slot-name
		  #'(lambda (fetch-query)
		      (apply #'plan-many-to-one-fetch
			     fetch-query slot-name alias foreign-key
			     (get-class-mapping reference-class-name)))
		  fetch-references)
	   (append foreign-key-columns columns)))))))

(defun join-one-to-many (join-path root-primary-key foreign-key
			 class-name &key table-name primary-key
				      properties one-to-many-mappings
				      many-to-one-mappings
				      superclass-mappings
				      subclass-mappings)
  (let* ((alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'list root-primary-key
			(append-alias alias foreign-key)))))
    (join-class alias class-name join-path primary-key properties
		one-to-many-mappings many-to-one-mappings
		superclass-mappings subclass-mappings)))

(defun join-one-to-many (join-path root-primary-key foreign-key
			 class-name &key table-name primary-key
				      properties one-to-many-mappings
				      many-to-one-mappings
				      superclass-mappings
				      subclass-mappings)
  (let* ((root-primary-key
	  (reduce #'(lambda (primary-key column)
		      (list* (apply fetch-query column)
			     primary-key))
		  root-primary-key :initial-value nil))
	 (alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'list root-primary-key
			(append-alias alias foreign-key)))))
    (plan-class alias class-name table-join primary-key
		properties one-to-many-mappings
		many-to-one-mappings superclass-mappings
		subclass-mappings)))

(defun join-one-to-many-mappings (join-path primary-key
				  &optional one-to-many-mapping
				  &rest one-to-many-mappings)
  (when (not (null many-to-one-mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key serializer
				   deserializer)
	one-to-many-mapping
      (declare (ignore serializer deserializer))
      (acons slot-name
	     #'(lambda ()
		 (apply #'join-one-to-many
			primary-key foreign-key
			(get-class-mapping reference-class-name)))
	     (apply #'join-one-to-many
		    join-path primary-key one-to-many-mappings)))))

(defun fetch-many-to-one (fetch-query slot-name alias foreign-key
			  class-name &key table-name primary-key
				       properties
				       one-to-many-mappings
				       many-to-one-mappings
				       superclass-mappings
				       subclass-mappings)
  (let* ((foreign-key
	  (reduce #'(lambda (foreign-key column)
		      (list* (funcall fetch-query alias column)
			     foreign-key))
		  foreign-key :initial-value nil))
	 (alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'list foreign-key
			(append-alias alias primary-key)))))
    (multiple-value-bind (selector join-references fetch-references)
	(fetch-class alias class-name table-join primary-key
		     properties one-to-many-mappings
		     many-to-one-mappings superclass-mappings
		     subclass-mappings)
      (declare (ignore join-references))
      (multiple-value-bind (columns from-clause object-loader)
	  (funcall selector)
	(values columns
		from-clause
		#'(lambda (object rows)
		    (setf (slot-value object slot-name)
			  (funcall object-loader (first rows))))
		fetch-references)))))

(defun fetch-many-to-one-mappings (alias &optional many-to-one-mapping
				   &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (multiple-value-bind (join-references fetch-references columns)
	(apply #'plan-many-to-one-mappings alias many-to-one-mappings)
      (destructuring-bind (slot-name
			   &key reference-class-name foreign-key)
	  many-to-one-mapping
	(multiple-value-bind (foreign-key-columns foreign-key-loader)
	    (plan-key alias foreign-key)
	  (values
	   (acons slot-name
		  #'(lambda ()
		      (apply #'plan-many-to-one-join
			     alias foreign-key
			     (get-class-mapping reference-class-name)))
		  join-references)
	   (acons slot-name
		  #'(lambda (fetch-query)
		      (apply #'plan-many-to-one-fetch
			     fetch-query slot-name alias foreign-key
			     (get-class-mapping reference-class-name)))
		  fetch-references)
	   (append foreign-key-columns columns)))))))

(defun fetch-one-to-many (fetch-query slot-name root-primary-key
			  foreign-key serializer class-name
			  &key table-name primary-key properties
			    one-to-many-mappings many-to-one-mappings
			    superclass-mappings subclass-mappings)
  (let* ((root-primary-key
	  (reduce #'(lambda (primary-key column)
		      (list* (apply fetch-query column)
			     primary-key))
		  root-primary-key :initial-value nil))
	 (alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'list root-primary-key
			(append-alias alias foreign-key)))))
    (plan-class alias class-name table-join primary-key
		properties one-to-many-mappings
		many-to-one-mappings superclass-mappings
		subclass-mappings)))

(defun fetch-one-to-many-mappings (primary-key &optional mapping
				   &rest mappings)
  (when (not (null mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key serializer
				   deserializer)
	one-to-many-mapping
      (declare (ignore serializer))
      (acons slot-name
	     #'(lambda (fetch-query)
		 (apply #'getch-one-to-many fetch-query
			slot-name primary-key foreign-key
			serializer
			(get-class-mapping reference-class-name)))
	     (apply #'fetch-one-to-many-mappings
		    join-path primary-key mappings)))))
 
(defun register-object (class primary-key object objects)
  (setf (gethash primary-key
		 (ensure-gethash class objects
				 (make-hash-table :test #'equal)))
	object))

(defun fetch-slots (class alias table-join primary-key properties
		    one-to-many-mappings many-to-one-mappings
		    superclass-mappings)
  (multiple-value-bind (superclasses-fetch-references
			superclasses-columns
			superclasses-from-clause
			superclasses-loaders)
      (fetch-superclasses alias superclass-mappings)
    (multiple-value-bind (property-columns property-loaders)
	(apply #'fetch-properties alias properties)
      (multiple-value-bind (many-to-one-fetched-references
			    foreign-key-columns)
	  (apply #'fetch-many-to-one alias many-to-one-mappings)
	(values
	 (append many-to-one-fetched-references
		 (apply #'plan-one-to-many-fetchings
			primary-key one-to-many-mappings)
		 superclasses-fetched-references)
	 (append primary-key-columns
		 property-columns
		 superclasses-columns)
	 (list* table-join superclasses-from-clause)
	 (list* #'(lambda (objects object row)
		    (dolist (loader property-loaders)
		      (funcall loader object row))
		    (register-object class
				     (funcall primary-key-loader row)
				     object objects))
		superclasses-loaders))))))

(defun fetch-superclass (subclass-alias
			 &key class-name table-name primary-key
			   foreign-key properties one-to-many-mappings
			   many-to-one-mappings superclass-mappings)
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
    (fetch-slots class alias table-join primary-key properties
		 one-to-many-mappings many-to-one-mappings
		 superclass-mappings)))

(defun fetch-superclasses (alias &optional superclass-mapping
			   &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (superclass-fetched-references
			  superclass-columns
			  superclass-from-clause
			  superclass-loaders)
	(apply #'fetch-superclass alias superclass-mapping))
    (multiple-value-bind (superclasses-fetched-references
			  superclasses-columns
			  superclasses-from-clause
			  superclasses-loaders)
	(apply #'fetch-superclasses alias superclass-mappings)
      (values
       (append superclass-fetched-references
	       superclasses-fetched-references)
       (append superclass-columns
	       superclasses-columns)
       (append superclass-from-clause
	       superclasses-from-clause)
       (append superclass-loaders
	       superclasses-loaders)))))

(defun fetch-class (class alias table-join primary-key
		    primary-key-columns primary-key-loader properties
		    one-to-many-mappings many-to-one-mappings
		    superclass-mappings subclass-mappings)
  (multiple-value-bind (primary-key-columns primary-key-loader)
      (apply #'plan-key alias primary-key)
    (multiple-value-bind (fetched-references
			  columns from-clause slot-loader)
	(fetch-slots class alias table-join primary-key
		     primary-key-columns primary-key-loader
		     properties one-to-many-mappings
		     many-to-one-mappings superclass-mappings)
      (multiple-value-bind (subclass-fetched-references
			  subclasses-columns
			  subclasses-from-clause loader)
	  (fetch-subclasses primary-key-columns subclass-mappings)
	(values
	 (append fetched-references
		 subclass-fetched-references)
	 (append columns subclasses-columns)
	 (append from-class subclasses-from-clause))
	#'(lambda (objects row &rest fetch-references)
	    (when (notevery #'null (funcall #'primary-key-loader row))
	      (reduce #'(lambda (object loader)
			  (funcall loader objects object row))
		      superclass-loaders
		      :initial-value
		      (or (some #'(lambda (loader)
				    (funcall loader objects row))
				subclass-loaders)
			  (allocate-instance class)))))))))

(defun fetch-subclass (superclass-primary-key class-name
		       &key primary-key table-name foreign-key
			 properties one-to-many-mappings
			 many-to-one-mappings superclass-mappings
			 subclass-mappings)
  (let* ((class (find-class class-name))
	 (alias (make-alias))
	 (foreign-key (append-alias alias foreign-key))
	 (table-join
	  (list* :left-join table-name alias
		 (mapcar #'list superclass-primary-key foreign-key)))
	 (primary-key (append-alias alias primary-key)))
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-key alias primary-key)
      (fetch-class class alias table-join primary-key
		   primary-key-columns primary-key-loader properties
		   one-to-many-mappings many-to-one-mappings
		   superclass-mappings subclass-mappings))))
	

(defun fetch-subclasses (superclass-primary-key
			 &optional subclass-mapping
			 &rest subclass-mappings)
  (when (not (null subclass-mapping))
    (multiple-value-bind (subclass-fetched-references
			  subclass-columns
			  subclass-from-clause
			  subclass-loaders)
	(apply #'plan-subclass-mappings
	       superclass-primary-key subclass-mappings)
      (multiple-value-bind (subclasses-fetched-references
			    subclasses-columns
			    subclasses-from-clause
			    subclasses-loader)
	  (apply #'plan-subclass-mapping
		 superclass-primary-key subclass-mapping)
	(values
	 (append subclass-fetched-references
		 subclasses-fetched-references)
	 (append subclass-columns
		 subclasses-columns)
	 (append subclass-from-clause
		 subclasses-from-clause)
	 (list* subclass-loader
		subclasses-loaders))))))

(defun join-superclass (subclass-alias join-path
			&key class-name table-name primary-key
			  foreign-key properties one-to-many-mappings
			  many-to-one-mappings superclass-mappings)
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
	(apply #'plan-key alias primary-key)
      (join-class class alias (list* table-join join-path) primary-key
		  primary-key-columns primary-key-loader properties
		  one-to-many-mappings many-to-one-mappings
		  superclass-mappings))))

(defun join-superclasses (alias join-path
			  &optional superclass-mapping
			  &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (superclasses-properties
			  superclasses-joined-references)
	(apply #'join-superclasses alias
	       join-path superclass-mappings)
      (multiple-value-bind (superclass-properties
			    superclass-joined-references)
	  (apply #'join-superclass alias
		 join-path superclass-mapping))
      (values
       (append superclass-properties
	       superclasses-properties)
       (append superclass-joined-references
	       superclasses-joined-references)))))

(defun join-class (class alias join-path primary-key
		   primary-key-columns primary-key-loader properties
		   one-to-many-mappings many-to-one-mappings
		   superclass-mappings)
  (multiple-value-bind (superclasses-properties
			superclasses-joined-references)
      (apply #'join-superclasses alias join-path superclass-mappings)
    (multiple-value-bind (properties property-columns property-loaders)
	(apply #'plan-properties alias join-path properties)
      (values
	 (append properties superclasses-properties)
	 (append (apply #'join-many-to-one
			alias join-path many-to-one-mappings)
		 (apply #'join-one-to-many-joins
			primary-key join-path one-to-many-mappings)
		 superclass-joined-references)))))

(defun plan-class (alias class-name join-path primary-key
		   properties one-to-many-mappings
		   many-to-one-mappings superclass-mappings
		   subclass-mappings)
  (let ((class (find-class class-name))
	(primary-key (append-alias alias primary-key)))
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-key alias primary-key)
      (multiple-value-bind (joined-properties joined-references)
	  (join-class class alias (list table-join) primary-key
		      primary-key-columns primary-key-loader
		      properties one-to-many-mappings
		      many-to-one-mappings superclass-mappings)
	(multiple-value-bind (fetched-references
			      fetched-columns
			      fetched-from-clause class-loader)
	    (fetch-class class alias table-join primary-key
			 primary-key-columns primary-key-loader
			 properties one-to-many-mappings
			 many-to-one-mappings superclass-mappings)
	  (multiple-value-bind (subclass-fetch-references
				subclasses-columns
				subclasses-from-clause
				subclasses-loaders)
	      (apply #'fetch-subclasses
		     primary-key-columns subclass-mappings)
	    (values joined-properties
		    joined-references
		    (append fetched-references
			    subclasses-fetched-references)
		    (append fetched-columns
			    subclasses-fetched-columns)
		    (append fetched-from-clause
			    subclasses-from-clause)
		    #'(lambda (objects row &options fetch-references)
			(when (notevery #'null (funcall #'primary-key-loader row))
			  (reduce #'(lambda (object loader)
				      (funcall loader objects object row))
				  superclass-loaders
				  :initial-value
				  (or (some #'(lambda (loader)
						(funcall loader objects row))
					    subclass-loaders)
				      (allocate-instance class))))))))))))

;;(values
;; #'(lambda (&optional (property-reader nil name-present-p))
;;     (if name-present-p
;;	 (funcall
;;	  (rest
;;	   (assoc (get-slot-name class property-reader)
;;		  properties)))
;;	 (values columns from-clause loader)))
;; #'(lambda (reader mode)
;;     (funcall (rest
;;	       (assoc (get-slot-name class reader)
;;		      (if (eq mode 'join)
;;			  join-references
;;			  fetch-references)))
;;	      mode))))))))

(defun plan-root-class-mapping (alias class-name
				&key table-name primary-key properties
				  one-to-many-mappings
				  many-to-one-mappings
				  superclass-mappings
				  subclass-mappings)
  (plan-class alias class-name (list table-name alias)
	      primary-key properties one-to-many-mappings
	      many-to-one-mappings superclass-mappings
	      subclass-mappings))
;; add value for fetch-refernces
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
(defun make-query (select-list where-clause order-by-clause
		   having-clause limit offset)
  (let (select-list
	from-clause
	where-clause
	group-by-clause
	having-clause
	order-by-clause)
  (lambda (&optional table-alias column-name)
    (if (and table-alias column-name)
	(list table-alias column-name)
	(values select-list
		from-clause
		where-clause
		group-by-clause
		having-clause
		order-by-clause
		limit
		offset)))))

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
	      (compute-clause joined-references join selectors))
	     (query
	      (make-query select-list
			  (compute-clause joined-references where)
			  (compute-clause select-list order-by)
			  (compute-clause select-list having)
			  limit
			  offset)))
	(reduce (lambda (query fetched-reference)
		  (funcall fetched-reference query))
		(compute-clause select-list fetch)
		:initial-value
		(if (not (null (and fetch (or limit offset))))
		    (make-subquery query)
		    query))))))
