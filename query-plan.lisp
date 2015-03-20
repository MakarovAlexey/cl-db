(in-package #:cl-db)

(defun make-expression (&key properties expression count
			  count-from-clause select-list from-clause
			  group-by-clause loader fetch join)
  #'(lambda (context)
      (cond ((eq context :property) properties)
	    ((eq context :expression) expression)
	    ((eq context :count) count)
	    ((eq context :count-from-clause)
	     (or count-from-clause from-clause))
	    ((eq context :select-list) select-list)
	    ((eq context :from-clause) from-clause)
	    ((eq context :group-by-clause) group-by-clause)
	    ((eq context :loader) loader)
	    ((eq context :fetch) fetch)
	    ((eq context :join) join))))

(defun expression-of (expression)
  (if (functionp expression)
      (funcall expression :expression)
      expression))

(defun properties-of (class-mapping)
  (funcall class-mapping :property))

(defun count-expression-of (expression)
  (funcall expression :count))

(defun count-from-clause-of (expression)
  (funcall expression :count-from-clause))

(defun select-list-of (expression)
  (funcall expression :select-list))

(defun from-clause-of (expression)
  (when (functionp expression)
    (funcall expression :from-clause)))

(defun group-by-clause-of (expression)
  (when (functionp expression)
    (funcall expression :group-by-clause)))

(defun loader (expression)
  (funcall expression :loader))

(defun fetch-references-of (class-mapping)
  (funcall class-mapping :fetch))

(defun join-references-of (class-mapping)
  (funcall class-mapping :join))

(defun make-value-loader (alias)
  #'(lambda (row)
      (rest
       (assoc alias row :test #'string=))))

(defvar *table-index*)

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun plan-column (table-alias column-name)
  (let ((alias (make-alias column-name))
	(column-expression (list #'write-column column-name table-alias)))
    (values (cons column-expression alias) ;; select item alias))
	    (make-value-loader alias))))

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

(defun find-slot-name (class reader-name)
  (or
   (find-if #'(lambda (slot-definition)
		(find reader-name
		      (slot-definition-readers slot-definition)))
	    (class-direct-slots class))
   (find-slot-name (find-if #'(lambda (class)
				(find-slot-name class reader-name))
			    (class-direct-superclasses class))
		   reader-name)))

(defun get-slot-name (class reader)
  (let ((slot-definition
	 (find-slot-name class (generic-function-name reader))))
    (when (null slot-definition)
      (error "Slot with reader ~a not found for class ~a"
	     reader (class-name class)))
    (slot-definition-name slot-definition)))

(defun make-property-loader (slot-name column-loader)
  #'(lambda (object row)
      (setf
       (property-commited-value object slot-name)
       (funcall column-loader row))))

(defun plan-properties (alias &optional property &rest properties)
  (when (not (null property))
    (multiple-value-bind (columns loaders)
	(apply #'plan-properties alias properties)
      (destructuring-bind (slot-name column-name column-type)
	  property
	(declare (ignore column-type))
	(multiple-value-bind (column column-loader)
	    (plan-column alias column-name)
	  (values
	   (list* column columns)
	   (list* (make-property-loader slot-name column-loader)
		  loaders)))))))

(defun join-properties (join-path alias
			&optional property &rest properties)
  (when (not (null property))
    (multiple-value-bind (properties columns loaders)
	(apply #'join-properties join-path alias properties)
      (destructuring-bind (slot-name column-name column-type)
	  property
	(declare (ignore column-type))
	(multiple-value-bind (column column-loader)
	    (plan-column alias column-name)
	  (let ((from-clause 
		 (join-path-append
		  (rest join-path)
		  (first join-path))))
	    (values
	     (acons slot-name
		    (make-expression :expression (first column)
				     :count (first column)
				     :select-list (list column)
				     :from-clause from-clause
				     :group-by-clause (list (first column))
				     :loader column-loader)
		    properties)
	     (list* column columns)
	     (list* (make-property-loader slot-name column-loader)
		    loaders))))))))

(defun join-many-to-one (join-path root-alias foreign-key class-name
			 &key table-name primary-key properties
			   one-to-many-mappings
			   many-to-one-mappings
			   superclass-mappings subclass-mappings)
  (let* ((alias (make-alias "join"))
	 (table-join
	  (list #'write-left-join table-name alias
		(mapcar #'(lambda (fk-column pk-column)
			    (list (first fk-column)
				  (first pk-column)))
			(apply #'plan-key root-alias foreign-key)
			(apply #'plan-key alias primary-key)))))
    (plan-class alias class-name table-join join-path primary-key
		properties one-to-many-mappings many-to-one-mappings
		superclass-mappings subclass-mappings)))

(defun join-many-to-one-mappings (join-path alias 
				  &optional many-to-one-mapping
				  &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key)
	many-to-one-mapping
      (acons slot-name
	     #'(lambda ()
		 (apply #'join-many-to-one
			join-path alias foreign-key
			(get-class-mapping reference-class-name)))
	     (apply #'join-many-to-one-mappings
		    join-path alias many-to-one-mappings)))))

(defun join-one-to-many (join-path root-primary-key foreign-key
			 class-name &key table-name primary-key
				      properties one-to-many-mappings
				      many-to-one-mappings
				      superclass-mappings
				      subclass-mappings)
  (let* ((alias (make-alias "join"))
	 (table-join
	  (list #'write-left-join table-name alias
		(mapcar #'(lambda (pk-column fk-column)
			    (list (first pk-column)
				  (first fk-column)))
			root-primary-key
			(apply #'plan-key alias foreign-key)))))
    (plan-class alias class-name table-join join-path primary-key
		properties one-to-many-mappings many-to-one-mappings
		superclass-mappings subclass-mappings)))

(defun join-one-to-many-mappings (join-path primary-key
				  &optional one-to-many-mapping
				  &rest one-to-many-mappings)
  (when (not (null one-to-many-mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key serializer
				   deserializer)
	one-to-many-mapping
      (declare (ignore serializer deserializer))
      (acons slot-name
	     #'(lambda ()
		 (apply #'join-one-to-many
			join-path primary-key foreign-key
			(get-class-mapping reference-class-name)))
	     (apply #'join-one-to-many-mappings
		    join-path primary-key one-to-many-mappings)))))

(defun append-children (appended-joins join-fn table-name alias on-clause &rest joins)
  (list* join-fn table-name alias on-clause
	 (reduce #'(lambda (result appended-join)
		     (apply #'join-append result appended-join))
		 appended-joins :initial-value joins)))

(defun join-append (joins join-fn table-name alias on-clause &rest appended-joins)
  (let ((join (find alias joins :key #'third)))
    (if (not (null join))
	(list*
	 (apply #'append-children appended-joins join)
	 (remove alias joins :key #'third))
	(list* (list* join-fn table-name alias on-clause appended-joins)
	       joins))))

(defun root-append (appended-joins table-reference-fn table-name alias &rest joins)
  (list* table-reference-fn table-name alias
	 (reduce #'(lambda (result join)
		     (apply #'join-append result join))
		 appended-joins :initial-value joins)))

(defun from-clause-append (from-clause table-reference-fn table-name alias &rest joins)
  (let ((root
	 (or
	  (find alias from-clause :key #'third)
	  (find #'write-subquery from-clause :key #'first))))
    (if (not (null root))
	(list*
	 (apply #'root-append joins root)
	 (remove (third root) from-clause :key #'third))
	(list* (list* table-reference-fn table-name alias joins)
	       from-clause))))

(defun query-append (query &key select-list from-clause where-clause
			     (group-by-clause nil group-by-present-p)
			     having-clause order-by-clause
			     limit offset)
  (multiple-value-bind (query-select-list
			query-from-clause
			query-where-clause
			query-group-by-clause
			query-having-clause
			query-order-by
			query-limit
			query-offset)
      (when (not (null query))
	(funcall query))
    (let ((select-list
	   (append select-list query-select-list)))
      #'(lambda (&optional expression)
	  (if (not (null expression))
	      (rassoc expression select-list)
	      (values select-list
		      (if (not (null from-clause))
			  (apply #'from-clause-append
				 query-from-clause from-clause)
			  query-from-clause)
		      (if (not (null where-clause))
			  (list* where-clause query-where-clause)
			  query-where-clause)
		      (if (not (null group-by-present-p))
			  (list* group-by-clause query-group-by-clause)
			  query-group-by-clause)
		      (if (not (null having-clause))
			  (list* having-clause query-having-clause)
			  query-having-clause)
		      (if (not (null order-by-clause))
			  (list* order-by-clause query-order-by)
			  query-order-by)
		      (or limit query-limit)
		      (or offset query-offset)))))))

(defun compute-fetch (query loader
		      &optional reference-fetching
		      &rest reference-fetchings)
  (if (not (null reference-fetching))
      (multiple-value-bind (query loader)
	  (funcall reference-fetching query loader)
	(apply #'compute-fetch query loader reference-fetchings))
      (values query loader)))

(defun loader-append (loader reference-loader)
  #'(lambda (row result-set &rest reference-loaders)
      (apply loader row result-set
	     (list* reference-loader
		    reference-loaders))))

(defun fetch-many-to-one (query loader fetch join-path root-class-name
			  slot-name foreign-key class-name
			  &key table-name primary-key properties
			    one-to-many-mappings many-to-one-mappings
			    superclass-mappings subclass-mappings)
  (let ((alias (make-alias "fetch")))
    (multiple-value-bind (primary-key pk-loader)
	(apply #'plan-key alias primary-key)
      (let ((table-join
	     (list #'write-left-join table-name alias
		   (mapcar #'(lambda (fk-column pk-column)
			       (list (first fk-column)
				     (first pk-column)))
			   foreign-key
			   primary-key))))
	(multiple-value-bind (columns
			      from-clause
			      reference-loader
			      fetch-references)
	    (fetch-object class-name alias table-join join-path
			  primary-key pk-loader properties
			  one-to-many-mappings many-to-one-mappings
			  superclass-mappings subclass-mappings)
	  (apply #'compute-fetch
		 (query-append query
			       :select-list columns
			       :from-clause from-clause
			       :group-by-clause columns)
		 (loader-append loader
				#'(lambda (object object-rows fetched-references)
				    (when (typep object root-class-name)
				      (setf
				       (many-to-one-commited-value object slot-name)
				       (funcall reference-loader
						(first object-rows)
						object-rows
						fetched-references)))))
		 (when (functionp fetch)
		   (mapcar #'funcall
			   (multiple-value-list
			    (funcall fetch fetch-references))))))))))

(defun fetch-many-to-one-mappings (join-path class-name alias
				   &optional many-to-one-mapping
				   &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (multiple-value-bind (references columns)
	(apply #'fetch-many-to-one-mappings
	       join-path class-name alias many-to-one-mappings)
      (destructuring-bind (slot-name
			   &key reference-class-name foreign-key)
	  many-to-one-mapping
	(multiple-value-bind (foreign-key-columns foreign-key-loader)
	    (apply #'plan-key alias foreign-key)
	  (declare (ignore foreign-key-loader))
	  (values
	   (acons slot-name
		  #'(lambda (query loader fetch)
		      (apply #'fetch-many-to-one
			     query loader fetch join-path class-name slot-name
			     foreign-key-columns
			     (get-class-mapping reference-class-name)))
		  references)
	   (append foreign-key-columns columns)))))))

(defun alias (expression)
  (rest expression))

(defun fetch-one-to-many (query loader fetch join-path root-class-name
			  slot-name root-primary-key foreign-key
			  serializer class-name
			  &key table-name primary-key properties
			    one-to-many-mappings many-to-one-mappings
			    superclass-mappings subclass-mappings)
  (let* ((root-primary-key
	  (reduce #'(lambda (primary-key alias)
		      (list* (funcall query alias) primary-key))
		  root-primary-key :key #'alias :initial-value nil))
	 (alias (make-alias "fetch"))
	 (table-join
	  (list #'write-left-join table-name alias
		(mapcar #'(lambda (pk-column fk-column)
			    (list (first pk-column)
				  (first fk-column)))
			root-primary-key
			(apply #'plan-key alias foreign-key)))))
    (multiple-value-bind (primary-key pk-loader)
	(apply #'plan-key alias primary-key)
      (multiple-value-bind (columns
			    from-clause
			    reference-loader
			    fetch-references)
	  (fetch-object class-name alias table-join join-path
			primary-key pk-loader properties
			one-to-many-mappings many-to-one-mappings
			superclass-mappings subclass-mappings)
	(apply #'compute-fetch
	       (query-append query
			     :select-list columns
			     :from-clause from-clause
			     :group-by-clause columns)
	       (loader-append loader
			      #'(lambda (object object-rows fetched-references)
				  (when (typep object root-class-name)
				    (setf
				     (one-to-many-commited-value object slot-name)
				     (apply serializer
					    (remove-duplicates
					     (mapcar #'(lambda (row)
							 (funcall reference-loader
								  row
								  object-rows
								  fetched-references))
						     object-rows)))))))
	       (when (functionp fetch)
		 (mapcar #'funcall
			 (multiple-value-list
			  (funcall fetch fetch-references)))))))))

(defun fetch-one-to-many-mappings (join-path class-name primary-key
				   &optional mapping &rest mappings)
  (when (not (null mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key serializer
				   deserializer)
	mapping
      (declare (ignore deserializer))
      (acons slot-name
	     #'(lambda (query loader fetch)
		 (apply #'fetch-one-to-many query loader fetch join-path
			class-name slot-name primary-key foreign-key
			serializer
			(get-class-mapping reference-class-name)))
	     (apply #'fetch-one-to-many-mappings
		    join-path class-name primary-key mappings)))))

(defun fetch-slots (class-name alias table-join join-path
		    primary-key-columns primary-key-loader properties
		    one-to-many-mappings many-to-one-mappings
		    superclass-mappings)
  (multiple-value-bind (superclasses-fetched-references
			superclasses-columns
			superclasses-from-clause
			superclasses-loaders)
      (apply #'fetch-superclasses join-path alias superclass-mappings)
    (multiple-value-bind (property-columns property-loaders)
	(apply #'plan-properties alias properties)
      (multiple-value-bind (many-to-one-fetched-references
			    foreign-key-columns)
	  (apply #'fetch-many-to-one-mappings
		 join-path class-name alias many-to-one-mappings)
	(values
	 (append many-to-one-fetched-references
		 (apply #'fetch-one-to-many-mappings
			join-path class-name primary-key-columns
			one-to-many-mappings)
		 superclasses-fetched-references)
	 (append primary-key-columns
		 property-columns
		 foreign-key-columns
		 superclasses-columns)
	 (append table-join superclasses-from-clause)
	 (list* #'(lambda (object row)
		    (register-object object
				     class-name
				     (funcall primary-key-loader row))
		    (dolist (loader property-loaders object)
		      (funcall loader object row)))
		superclasses-loaders))))))

(defun fetch-superclass (join-path subclass-alias
			 &key class-name table-name primary-key
			   foreign-key properties one-to-many-mappings
			   many-to-one-mappings superclass-mappings)
  (let* ((alias
	  (make-alias "fetch"))
	 (foreign-key
	  (apply #'plan-key subclass-alias foreign-key)))
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-key alias primary-key)
      (let ((table-join
	     (list #'write-inner-join table-name alias
		   (mapcar #'(lambda (pk-column fk-column)
			       (list (first pk-column)
				     (first fk-column)))
			   primary-key-columns
			   foreign-key))))
	(fetch-slots class-name alias table-join
		     (list* table-join join-path) primary-key-columns
		     primary-key-loader properties
		     one-to-many-mappings many-to-one-mappings
		     superclass-mappings)))))

(defun fetch-superclasses (join-path alias
			   &optional superclass-mapping
			   &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (superclass-fetched-references
			  superclass-columns
			  superclass-from-clause
			  superclass-loaders)
	(apply #'fetch-superclass
	       join-path alias superclass-mapping)
      (multiple-value-bind (superclasses-fetched-references
			    superclasses-columns
			    superclasses-from-clause
			    superclasses-loaders)
	  (apply #'fetch-superclasses
		 join-path alias superclass-mappings)
	(values
	 (append superclass-fetched-references
		 superclasses-fetched-references)
	 (append superclass-columns
		 superclasses-columns)
	 (append superclass-from-clause
		 superclasses-from-clause)
	 (append superclass-loaders
		 superclasses-loaders))))))

;; Carefully implement subclass dynamic association fetching (with
;; class specification)
(defun load-object (class primary-key row
		    superclass-loaders subclass-loaders)
  (let ((object (or (some #'(lambda (loader)
			      (funcall loader row))
			  subclass-loaders)
		    (allocate-instance class))))
    (dolist (superclass-loader superclass-loaders object)
      (funcall superclass-loader object primary-key row))))

(defun fetch-class (class-name alias table-join join-path
		    primary-key-columns primary-key-loader properties
		    one-to-many-mappings many-to-one-mappings
		    superclass-mappings subclass-mappings)
  (let ((class (find-class class-name)))
    (multiple-value-bind (fetched-references
			  columns from-clause superclass-loaders)
	(fetch-slots class-name alias table-join (list* table-join join-path)
		     primary-key-columns primary-key-loader properties
		     one-to-many-mappings many-to-one-mappings
		     superclass-mappings)
      (multiple-value-bind (subclasses-fetched-references
			    subclasses-columns
			    subclasses-from-clause subclass-loaders)
	  (apply #'fetch-subclasses
		 join-path primary-key-columns subclass-mappings)
	(values
	 (append fetched-references
		 subclasses-fetched-references)
	 (append columns subclasses-columns)
	 (append from-clause subclasses-from-clause)
	 #'(lambda (row)
	     (load-object class
			  (funcall primary-key-loader row)
			  row
			  superclass-loaders
			  subclass-loaders)))))))

(defun join-path-append (join-path from-clause)
  (reduce #'(lambda (from-clause table-join)
	      (append table-join (list from-clause)))
	  join-path :from-end nil
	  :initial-value from-clause))

(defun fetch-object (class-name alias table-join join-path primary-key
		     primary-key-loader properties
		     one-to-many-mappings many-to-one-mappings
		     superclass-mappings subclass-mappings)
  (multiple-value-bind (fetched-references
			fetched-columns
			fetched-from-clause class-loader)
      (fetch-class class-name alias table-join join-path
		   primary-key primary-key-loader
		   properties one-to-many-mappings
		   many-to-one-mappings superclass-mappings
		   subclass-mappings)
    (let ((class-loader
	   #'(lambda (row result-set reference-loaders)
	       (let ((primary-key
		      (funcall primary-key-loader row)))
		 (or
		  (get-object class-name primary-key)
		  (let ((object
			 (funcall class-loader row)))
		    (dolist (loader reference-loaders object)
		      (funcall loader object
			       (remove primary-key result-set
				       :key primary-key-loader
				       :test-not #'equal))))))))
	  (path (join-path-append join-path fetched-from-clause)))
      (values fetched-columns
	      path
	      class-loader
	      #'(lambda (reader)
		  (values (rest
			   (assoc (get-slot-name (find-class class-name)
						 reader)
				  fetched-references))
			  class-loader))))))

(defun fetch-subclass (join-path superclass-primary-key class-name
		       &key primary-key table-name foreign-key
			 properties one-to-many-mappings
			 many-to-one-mappings superclass-mappings
			 subclass-mappings)
  (let* ((alias (make-alias "fetch"))
	 (foreign-key (apply #'plan-key alias foreign-key))
	 (table-join
	  (list #'write-left-join table-name alias
		(mapcar #'(lambda (pk-column fk-column)
			    (list (first pk-column)
				  (first fk-column)))
			superclass-primary-key
			foreign-key))))
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-key alias primary-key)
      (multiple-value-bind (class-fetched-references
			    class-columns
			    class-from-clause
			    class-loader)
	  (fetch-class class-name alias table-join join-path
		       primary-key-columns primary-key-loader
		       properties one-to-many-mappings
		       many-to-one-mappings superclass-mappings
		       subclass-mappings)
	(values class-fetched-references
		class-columns
		class-from-clause
		#'(lambda (row)
		    (let ((primary-key
			   (funcall primary-key-loader row)))
		      (when (notevery #'null primary-key)
			(funcall class-loader primary-key row)))))))))

(defun fetch-subclasses (join-path superclass-primary-key
			 &optional subclass-mapping
			 &rest subclass-mappings)
  (when (not (null subclass-mapping))
    (multiple-value-bind (subclass-fetched-references
			  subclass-columns
			  subclass-from-clause
			  subclass-loader)
	(apply #'fetch-subclass
	       join-path superclass-primary-key subclass-mapping)
      (multiple-value-bind (subclasses-fetched-references
			    subclasses-columns
			    subclasses-from-clause
			    subclasses-loaders)
	  (apply #'fetch-subclasses
		 join-path superclass-primary-key subclass-mappings)
	(values
	 (append subclass-fetched-references
		 subclasses-fetched-references)
	 (append subclass-columns
		 subclasses-columns)
	 (list* subclass-from-clause
		subclasses-from-clause)
	 (list* subclass-loader
		subclasses-loaders))))))

(defun join-superclass (subclass-alias join-path
			&key class-name table-name primary-key
			  foreign-key properties one-to-many-mappings
			  many-to-one-mappings superclass-mappings)
  (let ((alias
	 (make-alias "join"))
	(foreign-key
	 (apply #'plan-key subclass-alias foreign-key)))
    (multiple-value-bind (primary-key primary-key-loader)
	(apply #'plan-key alias primary-key)
      (let ((table-join
	     (list #'write-inner-join table-name alias
		   (mapcar #'(lambda (pk-column fk-column)
			       (list
				(first pk-column)
				(first fk-column)))
			   primary-key
			   foreign-key))))
	(join-class alias table-join (list* table-join join-path)
		    class-name primary-key primary-key-loader
		    properties one-to-many-mappings
		    many-to-one-mappings superclass-mappings)))))

(defun join-superclasses (alias join-path
			  &optional superclass-mapping
			  &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (superclasses-properties
			  superclasses-joined-references
			  superclasses-fetched-references
			  superclasses-columns
			  superclasses-from-clause
			  superclasses-loaders)
	(apply #'join-superclasses alias
	       join-path superclass-mappings)
      (multiple-value-bind (superclass-properties
			    superclass-joined-references
			    superclass-fetched-references
			    superclass-columns
			    superclass-from-clause
			    superclass-loaders)
	  (apply #'join-superclass alias
		 join-path superclass-mapping)
	(values
	 (append superclass-properties
		 superclasses-properties)
	 (append superclass-joined-references
		 superclasses-joined-references)
	 (append superclass-fetched-references
		 superclasses-fetched-references)
	 (append superclass-columns
		 superclasses-columns)
	 (list* superclass-from-clause
		superclasses-from-clause)
	 (append superclass-loaders
		 superclasses-loaders))))))

(defun join-class (alias table-join join-path class-name
		   primary-key-columns primary-key-loader properties
		   one-to-many-mappings many-to-one-mappings
		   superclass-mappings)
  (multiple-value-bind (superclasses-properties
			superclasses-joined-references
			superclasses-fetched-references
			superclasses-columns
			superclasses-from-clause
			superclasses-loaders)
      (apply #'join-superclasses alias join-path superclass-mappings)
    (multiple-value-bind (properties property-columns property-loaders)
	(apply #'join-properties join-path alias properties)
      (multiple-value-bind (many-to-one-fetched-references
			    foreign-key-columns)
	  (apply #'fetch-many-to-one-mappings
		 join-path class-name alias many-to-one-mappings)
	(values
	 (append properties
		 superclasses-properties)
	 (append (apply #'join-many-to-one-mappings
			join-path alias many-to-one-mappings)
		 (apply #'join-one-to-many-mappings join-path
			primary-key-columns one-to-many-mappings)
		 superclasses-joined-references)
	 (append many-to-one-fetched-references
		 (apply #'fetch-one-to-many-mappings
			join-path class-name primary-key-columns
			one-to-many-mappings)
		 superclasses-fetched-references)
	 (append primary-key-columns
		 property-columns
		 foreign-key-columns
		 superclasses-columns)
	 (append table-join superclasses-from-clause)
	 (list* #'(lambda (object row)
		    (register-object object
				     class-name
				     (funcall primary-key-loader row))
		    (dolist (loader property-loaders object)
		      (funcall loader object row)))
		superclasses-loaders))))))

(defun plan-class (alias class-name table-join join-path primary-key
		   properties one-to-many-mappings
		   many-to-one-mappings superclass-mappings
		   subclass-mappings)
  (multiple-value-bind (primary-key primary-key-loader)
      (apply #'plan-key alias primary-key)
    (multiple-value-bind (joined-properties
			  joined-references
			  fetched-references
			  fetched-columns
			  fetched-from-clause
			  superclasses-loaders)
	(join-class alias table-join (list* table-join join-path)
		    class-name primary-key primary-key-loader
		    properties one-to-many-mappings
		    many-to-one-mappings superclass-mappings)
      (multiple-value-bind (subclasses-fetch-references
			    subclasses-columns
			    subclasses-from-clause
			    subclasses-loaders)
	  (apply #'fetch-subclasses (list* table-join join-path)
		 primary-key subclass-mappings)
	(let* ((class (find-class class-name))
	       (class-loader
		#'(lambda (row result-set reference-loaders)
		    (let ((primary-key
			   (funcall primary-key-loader row)))
		      (or
		       (get-object class-name primary-key)
		       (let ((object
			      (load-object class primary-key row
					   superclasses-loaders
					   subclasses-loaders)))
			 (dolist (loader reference-loaders object)
			   (funcall loader object
				    (remove primary-key result-set
					    :key primary-key-loader
					    :test-not #'equal))))))))
	       (columns (append fetched-columns
				subclasses-columns))
	       (path (reverse (list* table-join join-path)))
	       (from-clause
		(join-path-append join-path
				  (append fetched-from-clause
					  subclasses-from-clause)))
	       (fetch-references
		(append fetched-references
			subclasses-fetch-references)))
	  (values (make-expression :properties
				   #'(lambda (reader)
				       (rest
					(assoc (get-slot-name class reader)
					       joined-properties)))
				   :select-list columns
				   :count (mapcar #'first primary-key)
				   :count-from-clause path
				   :from-clause from-clause
				   :group-by-clause (mapcar #'first columns)
				   :loader class-loader
				   :fetch
				   #'(lambda (reader)
				       (values (rest
						(assoc
						 (get-slot-name (find-class class-name)
								reader)
						 fetch-references))
					       class-loader))
				   :join #'(lambda (reader)
					     (funcall
					      (rest
					       (assoc (get-slot-name class reader)
						      joined-references)))))))))))

(defun plan-root-class-mapping (class-name &key table-name primary-key
				properties one-to-many-mappings
				many-to-one-mappings
				superclass-mappings subclass-mappings)
  (let ((alias (make-alias "root")))
    (plan-class alias class-name
		(list #'write-table-reference table-name alias)
		nil primary-key properties one-to-many-mappings
		many-to-one-mappings superclass-mappings
		subclass-mappings)))

(defun make-join-plan (mapping-schema class-name &rest class-names)
  (list* (apply #'plan-root-class-mapping
		(get-class-mapping class-name mapping-schema))
	 (reduce #'(lambda (class-name result)
		     (list* (apply #'plan-root-class-mapping
				   (get-class-mapping class-name mapping-schema))
			    result))
		 class-names
		 :from-end t
		 :initial-value nil)))
