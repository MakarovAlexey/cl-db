(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun get-class-mapping (class-name
			  &optional (mapping-schema *mapping-schema*))
  (or
   (assoc class-name mapping-schema)
   (error "class mapping for class ~a not found" class-name)))

(defun plan-column (table-alias column-name)
  (let ((alias (make-alias column-name))
	(column-expression
	 #'(lambda ()
	     (values column-name table-alias))))
    (values #'(lambda ()
		(values column-expression
			alias ;; select item alias
			(list column-expression))) ;; group by expression
	    #'(lambda (row) ;; loader
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
		      (find
		       (generic-function-name reader)
		       (slot-definition-readers slot-definition)))
		  (class-direct-slots class))))
    (when (null slot-definition)
      (error "Slot with reader ~a not found for class ~a"
	     reader (class-name class)))
    (slot-definition-name slot-definition)))

(defun plan-properties (alias &optional property &rest properties)
  (when (not (null property))
    (multiple-value-bind (columns loaders)
	(apply #'plan-properties alias properties)
      (destructuring-bind (slot-name column-name column-type)
	  property
	(declare (ignore column-type slot-name))
	(multiple-value-bind (column column-loader)
	    (plan-column alias column-name)
	  (values
	   (list* column columns)
	   (list* column-loader loaders)))))))

(defun join-properties (join-path alias
			&optional property &rest properties)
  (when (not (null property))
    (destructuring-bind (slot-name column-name column-type)
	property
      (declare (ignore column-type))
      (multiple-value-bind (column column-loader)
	  (plan-column alias column-name)
	(let ((loader #'(lambda (object row)
			  (setf
			   (slot-value object slot-name)
			   (funcall column-loader row)))))
	  (acons slot-name
		 #'(lambda ()
		     (values column (reverse join-path) loader))
		 (apply #'join-properties
			alias join-path properties)))))))

(defun join-many-to-one (join-path root-alias foreign-key class-name
			 &key table-name primary-key properties
			   one-to-many-mappings
			   many-to-one-mappings
			   superclass-mappings subclass-mappings)
  (let* ((alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'(lambda (fk-column pk-column)
			    (list
			     (funcall fk-column)
			     (funcall pk-column)))
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
  (let* ((alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'(lambda (pk-column fk-column)
			    (list pk-column (funcall fk-column)))
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

(defun fetch-many-to-one (query loader fetch root-class-name
			  slot-name foreign-key class-name
			  &key table-name primary-key properties
			    one-to-many-mappings many-to-one-mappings
			    superclass-mappings subclass-mappings)
  (let* ((alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'(lambda (fk-column pk-column)
			    (list fk-column (funcall pk-column)))
			foreign-key
			(apply #'plan-key alias primary-key)))))
    (multiple-value-bind (columns
			  from-clause
			  reference-loader
			  fetch-references)
	(fetch-object class-name alias table-join primary-key
		      properties one-to-many-mappings
		      many-to-one-mappings superclass-mappings
		      subclass-mappings)
      (apply #'compute-fetch
	     (query-append query
			   :select-list columns
			   :from-clause from-clause)
	     (loader-append loader
			    #'(lambda (object object-rows fetched-references)
				(when (typep object root-class-name)
				  (setf
				   (slot-value object slot-name)
				   (funcall reference-loader
					    (first object-rows)
					    object-rows
					    fetched-references)))))
	     (when (functionp fetch)
	       (mapcar #'funcall
		       (multiple-value-list
			(funcall fetch fetch-references))))))))

(defun fetch-many-to-one-mappings (class-name alias
				   &optional many-to-one-mapping
				   &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (multiple-value-bind (references columns)
	(apply #'fetch-many-to-one-mappings
	       class-name alias many-to-one-mappings)
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
			     query loader fetch class-name slot-name
			     (mapcar #'funcall foreign-key-columns)
			     (get-class-mapping reference-class-name)))
		  references)
	   (append foreign-key-columns columns)))))))

(defun fetch-one-to-many (query loader fetch root-class-name slot-name
			  root-primary-key foreign-key serializer
			  class-name &key table-name primary-key
				       properties one-to-many-mappings
				       many-to-one-mappings
				       superclass-mappings
				       subclass-mappings)
  (let* ((root-primary-key
	  (reduce #'(lambda (primary-key column)
		      (list* (funcall query column) primary-key))
		  root-primary-key :initial-value nil))
	 (alias (make-alias))
	 (table-join
	  (list :left-join table-name alias
		(mapcar #'(lambda (pk-column fk-column)
			    (list pk-column
				  (funcall fk-column)))
			root-primary-key
			(apply #'plan-key alias foreign-key)))))
    (multiple-value-bind (columns
			  from-clause
			  reference-loader
			  fetch-references)
	(fetch-object class-name alias table-join primary-key
		      properties one-to-many-mappings
		      many-to-one-mappings superclass-mappings
		      subclass-mappings)
      (apply #'compute-fetch
	     (query-append query
			   :select-list columns
			   :from-clause from-clause)
	     (loader-append loader
			    #'(lambda (object object-rows fetched-references)
				(when (typep object root-class-name)
				  (setf
				   (slot-value object slot-name)
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
			(funcall fetch fetch-references))))))))

(defun fetch-one-to-many-mappings (class-name primary-key
				   &optional mapping &rest mappings)
  (when (not (null mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key serializer
				   deserializer)
	mapping
      (declare (ignore deserializer))
      (acons slot-name
	     #'(lambda (query loader fetch)
		 (apply #'fetch-one-to-many query loader fetch
			class-name slot-name primary-key foreign-key
			serializer
			(get-class-mapping reference-class-name)))
	     (apply #'fetch-one-to-many-mappings
		    class-name primary-key mappings)))))
 
(defun register-object (class primary-key object
			&optional (objects *objects*))
  (setf (gethash primary-key
		 (ensure-gethash class objects
				 (make-hash-table :test #'equal)))
	object))

(defun fetch-slots (class-name alias table-join
		    primary-key-columns primary-key-loader properties
		    one-to-many-mappings many-to-one-mappings
		    superclass-mappings)
  (multiple-value-bind (superclasses-fetched-references
			superclasses-columns
			superclasses-from-clause
			superclasses-loaders)
      (apply #'fetch-superclasses alias superclass-mappings)
    (multiple-value-bind (property-columns property-loaders)
	(apply #'plan-properties alias properties)
      (multiple-value-bind (many-to-one-fetched-references
			    foreign-key-columns)
	  (apply #'fetch-many-to-one-mappings
		 class-name alias many-to-one-mappings)
	(values
	 (append many-to-one-fetched-references
		 (apply #'fetch-one-to-many-mappings class-name
			primary-key-columns one-to-many-mappings)
		 superclasses-fetched-references)
	 (append primary-key-columns
		 property-columns
		 foreign-key-columns
		 superclasses-columns)
	 (list* table-join superclasses-from-clause)
	 (list* #'(lambda (object row)
		    (register-object class-name
				     (funcall primary-key-loader row)
				     object)
		    (dolist (loader property-loaders object)
		      (funcall loader object row)))
		superclasses-loaders))))))

(defun fetch-superclass (subclass-alias
			 &key class-name table-name primary-key
			 foreign-key properties one-to-many-mappings
			 many-to-one-mappings superclass-mappings)
  (let* ((alias
	  (make-alias))
	 (foreign-key
	  (apply #'plan-key subclass-alias foreign-key)))
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-key alias primary-key)
      (fetch-slots class-name alias
		   (list* :inner-join table-name alias
			  (mapcar #'(lambda (pk-column fk-column)
				      (list
				       (funcall pk-column)
				       (funcall fk-column)))
				  primary-key-columns
				  foreign-key))
		   primary-key-columns primary-key-loader properties
		   one-to-many-mappings many-to-one-mappings
		   superclass-mappings))))

(defun fetch-superclasses (alias &optional superclass-mapping
			   &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (superclass-fetched-references
			  superclass-columns
			  superclass-from-clause
			  superclass-loaders)
	(apply #'fetch-superclass alias superclass-mapping)
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

(defun fetch-class (class-name alias table-join
		    primary-key-columns primary-key-loader properties
		    one-to-many-mappings many-to-one-mappings
		    superclass-mappings subclass-mappings)
  (let ((class (find-class class-name)))
    (multiple-value-bind (fetched-references
			  columns from-clause superclass-loaders)
	(fetch-slots class-name alias table-join
		     primary-key-columns primary-key-loader properties
		     one-to-many-mappings many-to-one-mappings
		     superclass-mappings)
      (multiple-value-bind (subclasses-fetched-references
			    subclasses-columns
			    subclasses-from-clause subclass-loaders)
	  (apply #'fetch-subclasses
		 primary-key-columns subclass-mappings)
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

(defun fetch-object (class-name alias table-join primary-key
		     properties one-to-many-mappings
		     many-to-one-mappings superclass-mappings
		     subclass-mappings)
  (multiple-value-bind (primary-key-columns primary-key-loader)
      (apply #'plan-key alias primary-key)
    (multiple-value-bind (fetched-references
			  fetched-columns
			  fetched-from-clause class-loader)
	(fetch-class class-name alias table-join
		     primary-key-columns primary-key-loader
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
					  :test-not #'equal)))))))))
	(values fetched-columns
		fetched-from-clause
		class-loader
		#'(lambda (reader)
		    (values (rest
			     (assoc (get-slot-name (find-class class-name)
						   reader)
				    fetched-references))
			    class-loader)))))))

(defun fetch-subclass (superclass-primary-key class-name
		       &key primary-key table-name foreign-key
			 properties one-to-many-mappings
			 many-to-one-mappings superclass-mappings
			 subclass-mappings)
  (let* ((alias (make-alias))
	 (foreign-key (apply #'plan-key alias foreign-key))
	 (table-join
	  (list* :left-join table-name alias
		 (mapcar #'(lambda (pk-column fk-column)
			     (list pk-column
				   (funcall fk-column)))
			 superclass-primary-key
			 foreign-key))))
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-key alias primary-key)
      (multiple-value-bind (class-fetched-references
			    class-columns
			    class-from-clause
			    class-loader)
	  (fetch-class class-name alias table-join
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

(defun fetch-subclasses (superclass-primary-key
			 &optional subclass-mapping
			 &rest subclass-mappings)
  (when (not (null subclass-mapping))
    (multiple-value-bind (subclass-fetched-references
			  subclass-columns
			  subclass-from-clause
			  subclass-loader)
	(apply #'fetch-subclass
	       superclass-primary-key subclass-mapping)
      (multiple-value-bind (subclasses-fetched-references
			    subclasses-columns
			    subclasses-from-clause
			    subclasses-loaders)
	  (apply #'fetch-subclasses
		 superclass-primary-key subclass-mappings)
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
  (declare (ignore class-name))
  (let* ((alias
	  (make-alias))
	 (foreign-key
	  (apply #'plan-key subclass-alias foreign-key))
	 (primary-key
	  (apply #'plan-key alias primary-key))
	 (table-join
	  (list* :inner-join table-name alias
		 (mapcar #'(lambda (pk-column fk-column)
			     (list
			      (funcall pk-column)
			      (funcall fk-column)))
			 primary-key
			 foreign-key))))
    (join-class alias (list* table-join join-path) primary-key
		properties one-to-many-mappings many-to-one-mappings
		superclass-mappings)))

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
		 join-path superclass-mapping)
	(values
	 (append superclass-properties
		 superclasses-properties)
	 (append superclass-joined-references
		 superclasses-joined-references))))))

(defun join-class (alias join-path primary-key properties
		   one-to-many-mappings many-to-one-mappings
		   superclass-mappings)
  (multiple-value-bind (superclasses-properties
			superclasses-joined-references)
      (apply #'join-superclasses alias join-path superclass-mappings)
    (values
     (append (apply #'join-properties
		    join-path alias properties)
	     superclasses-properties)
     (append (apply #'join-many-to-one-mappings
		    join-path alias many-to-one-mappings)
	     (apply #'join-one-to-many-mappings join-path
		    (mapcar #'funcall
			    (apply #'plan-key
				   alias primary-key))
		    one-to-many-mappings)
	     superclasses-joined-references))))

(defun plan-class (alias class-name table-join join-path primary-key
		   properties one-to-many-mappings
		   many-to-one-mappings superclass-mappings
		   subclass-mappings)
  (multiple-value-bind (joined-properties joined-references)
      (join-class alias (list* table-join join-path) primary-key
		  properties one-to-many-mappings many-to-one-mappings
		  superclass-mappings)
    (multiple-value-bind (fetched-columns
			  fetched-from-clause
			  class-loader
			  fetched-references)
	(fetch-object class-name alias table-join
		      primary-key properties one-to-many-mappings
		      many-to-one-mappings superclass-mappings
		      subclass-mappings)
      (let ((from-clause
	     (append (reverse join-path) fetched-from-clause))
	    (class (find-class class-name)))
	(values #'(lambda ()
		    (values #'(lambda (&optional (property-reader
						  nil name-present-p))
				(if name-present-p
				    (funcall
				     (rest
				      (assoc (get-slot-name class property-reader)
					     joined-properties)))
				    (values fetched-columns
					    from-clause
					    class-loader)))
			    fetched-references))
		#'(lambda (reader)
		    (funcall
		     (rest
		      (assoc (get-slot-name class reader)
			     joined-references)))))))))

(defun plan-root-class-mapping (class-name &key table-name primary-key
					     properties
					     one-to-many-mappings
					     many-to-one-mappings
					     superclass-mappings
					     subclass-mappings)
  (let ((alias (make-alias table-name)))
    (plan-class alias class-name (list table-name alias) nil
		primary-key properties one-to-many-mappings
		many-to-one-mappings superclass-mappings
		subclass-mappings)))

(defun make-join-plan (mapping-schema class-name &rest class-names)
  (multiple-value-bind (selectors rest-joined-references)
      (when (not (null class-names))
	(apply #'make-join-plan mapping-schema class-names))
    (multiple-value-bind (selector joined-references)
	(apply #'plan-root-class-mapping 
	       (get-class-mapping class-name mapping-schema))
      (values
       (list* selector selectors)
       (list* joined-references rest-joined-references)))))
