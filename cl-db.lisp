;;; cl-db.lisp

(in-package #:cl-db)

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (mapped-superclasses :initarg :mapped-superclasses
			:reader mapped-superclasses-of)
   (value-mappings :initarg :value-mapping
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)))

(defmethod initialize-instance :after ((instance class-mapping-definition)
				       &key many-to-one-mappings
				       one-to-many-mappings)
  (dolist (mapping (append many-to-one-mappings one-to-many-mappings))
    (setf (class-mapping-definition-of mapping) instance)))

(defun value-mapping-p (mapping)
  (typep mapping (find-class 'value-mapping-definition)))

(defun many-to-one-mapping-p (mapping)
  (typep mapping (find-class 'many-to-one-mapping-definition)))

(defun one-to-many-mapping-p (mapping)
  (typep mapping (find-class 'one-to-many-mapping-definition)))

(defun map-class (class-name table-name primary-key &key superclasses slots)
  (make-instance 'class-mapping-definition
		 :table-name table-name
		 :primary-key primary-key
		 :mapped-class (find-class class-name)
		 :mapped-superclasses (mapcar #'find-class superclasses)
		 :value-mapping (remove-if-not #'value-mapping-p slots)
		 :many-to-one-mappings (remove-if-not #'many-to-one-mapping-p slots)
		 :one-to-many-mappings (remove-if-not #'one-to-many-mapping-p slots)))

(defclass slot-mapping-definition ()
  ((class-mapping-definition :accessor class-mapping-definition-of)
   (slot-name :initarg :slot-name :reader slot-name-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defclass value-mapping-definition (slot-mapping-definition)
  ((columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defclass one-to-many-mapping-definition (slot-mapping-definition)
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defclass many-to-one-mapping-definition (slot-mapping-definition)
  ((class-mapping-definition :accessor class-mapping-definition-of)
   (mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun map-slot (slot-name mapping
		 &optional (marshaller #'list) (unmarshaller #'list))
  (funcall mapping slot-name marshaller unmarshaller))

(defun value (column &rest columns)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'value-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :columns-names (list* column columns))))

(defun one-to-many (class-name column &rest columns)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'one-to-many-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :mapped-class (find-class class-name)
		     :columns-names (list* column columns))))

(defun many-to-one (class-name column &rest columns)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'many-to-one-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :mapped-class (find-class class-name)
		     :columns-names (list* column columns))))

(defclass mapping-schema ()
  ((class-mappings :initform (make-hash-table)
		   :reader class-mappings-of)
   (tables :initform (make-hash-table)
	   :reader tables-of)))

(defun get-table (name mapping-schema)
  (gethash name (tables-of mapping-schema)))

(defun get-mapping (mapped-class mapping-schema)
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class (class-mappings-of mapping-schema))
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defclass class-mapping ()
  ((table :initarg :table
	  :reader table-of)
   (value-mappings :initarg :vaule-mappings
		   :reader value-mappings-of)
   (reference-mappings :initarg :reference-mappings
		       :reader reference-mappings-of)
   (subclasses-mappings :initarg :subclasses-mappings
			:reader subclasses-mappings-of)
   (superclasses-mappings :initarg :superclasses-mappings
			  :reader superclasses-mappings-of)))

(defclass association ()
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass unidirectional-many-to-one-association (association)
  ((many-to-one-direction :initarg :many-to-one-direction
			  :reader many-to-one-direction-of)))

(defclass unidirectional-one-to-many-association (association)
  ((one-to-many-direction :initarg :one-to-many-direction
			  :reader one-to-many-direction-of)))

(defclass bidirectional-association
    (unidirectional-many-to-one-association
     unidirectional-one-to-many-association)
  ())

(defclass value-column (column) ())

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defclass value-mapping (slot-mapping)
  ((columns :initarg :columns :reader columns-of)))

(defclass reference-mapping (slot-mapping)
  ((referenced-class-mapping :initarg :referenced-class-mapping
			     :reader referenced-class-mapping-of)
   (association :initarg :association
		:reader association-of)))

(defclass one-to-many-direction (reference-mapping) ())

(defclass many-to-one-mapping (reference-mapping) ())

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initarg :columns :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :reader foreign-keys-of)))

(defun get-column (name table)
  (gethash name (columns-of table))) 

(defclass column ()
  ((name :initarg :name :reader name-of)
   (type-name :initarg :type-name :reader type-name-of)))

(defclass foreign-key-column (column)
  ((foreign-keys :initarg :foreign-keys :reader foreign-keys-of)))

(defclass foreign-key ()
  ((table :initarg :table
	  :reader table-of)
   (columns :initarg :columns
	    :reader columns-of)
   (referenced-table :initarg :referenced-table
		     :reader referenced-table-of)))

(defun compute-columns (class-mapping-definition class-mapping-definitions)
  (let ((columns (make-hash-table :test #'equal)))
    (dolist (many-to-one-mapping (many-to-one-mappings-of class-mapping-definition))
      (dolist (column-name (columns-names-of many-to-one-mapping))
	(setf (gethash column-name columns)
	      (make-instance 'column :name column-name))))
    (let ((mapped-class (mapped-class-of class-mapping-definition)))
      (dolist (class-mapping-definition class-mapping-definitions columns)
	(dolist (one-to-many-mapping (remove-if-not #'(lambda (mapping)
							(eq mapped-class 
							    (mapped-class-of mapping)))
						    (one-to-many-mappings-of class-mapping-definition)))
	  (dolist (column-name (columns-names-of one-to-many-mapping))
	    (setf (gethash column-name columns)
		  (make-instance 'column :name column-name))))))
    (dolist (value-mapping (value-mappings-of class-mapping-definition))
      (dolist (column-name (columns-names-of value-mapping))
	(multiple-value-bind (column presentp)
	    (gethash column-name columns)
	  (if presentp
	      (error "Column ~a already mapped" column)
	      (setf (gethash column-name columns)
		    (make-instance 'column :name column-name))))))
    columns))

(defun compute-tables (class-mapping-definitions)
  (let ((tables (make-hash-table :test #'equal
				 :size (length class-mapping-definitions))))
    (dolist (class-mapping-definition class-mapping-definitions tables)
      (let* ((table-name (table-name-of class-mapping-definition))
	     (columns (compute-columns class-mapping-definition
				       class-mapping-definitions)))
	(setf (gethash table-name tables)
	      (make-instance 'table :name table-name
			     :columns columns
			     :primary-key (mapcar #'(lambda (name)
						      (gethash name columns))
						  (primary-key-of class-mapping-definition))))))))

(defun allocate-class-mappings (class-mapping-definitions)
  (let ((class-mappings (make-hash-table :size (length class-mapping-definitions))))
    (dolist (class-mapping-definition class-mapping-definitions class-mappings)
      (setf (gethash (mapped-class-of class-mapping-definition) class-mappings)
	    (allocate-instance (find-class 'class-mapping))))))

(defun reference-definition-key (reference-definition)
  (list* (mapped-class-of reference-definition)
	 (columns-names-of reference-definition)))

;; разделить на функции
(defun compute-reference-pairs (class-mapping-definitions)
  (let ((many-to-one-definitions (apply #'append
					(mapcar #'many-to-one-mappings-of
						class-mapping-definitions)))
	(one-to-many-definitions (apply #'append
					(mapcar #'one-to-many-mappings-of
						class-mapping-definitions))))
    (let ((associations (list)))
      (dolist (many-to-one-definition many-to-one-definitions)
	(when (null (assoc many-to-one-definition associations))
	  (setf associations
		(acons many-to-one-definition
		       (find (reference-definition-key many-to-one-definition)
			     one-to-many-definitions
			     :test #'equal
			     :key #'reference-definition-key)
		       associations))))
      (dolist (one-to-many-definition one-to-many-definitions)
	(when (null (rassoc one-to-many-definition associations))
	  (setf associations
		(acons (find (reference-definition-key one-to-many-definition)
			     many-to-one-definitions
			     :test #'equal
			     :key #'reference-definition-key)
		       one-to-many-definition
		       associations)))))))

(defgeneric compute-foreign-key (reference-definition
				 class-mapping-definitions
				 mapping-schema))

(defmethod compute-foreign-key ((many-to-one-definition many-to-one-mapping-definition)
				class-mapping-definitions
				mapping-schema)
  (let ((table (get-table
		(table-name-of
		 (class-mapping-definition-of many-to-one-definition))
		mapping-schema)))
    (make-instance 'foreign-key
		   :table table
		   :columns (mapcar #'(lambda (name)
					(get-column name table))
				    (columns-names-of many-to-one-definition))
		   :referenced-table (get-table
				      (table-name-of
				       (find (mapped-class-of many-to-one-definition)
					     class-mapping-definitions
					     :key #'mapped-class-of))
				      mapping-schema))))

(defmethod compute-foreign-key ((one-to-many-definition one-to-many-mapping-definition)
				class-mapping-definitions
				mapping-schema)
  (let ((table (get-table (table-name-of
			   (find (mapped-class-of one-to-many-definition)
				 class-mapping-definitions
				 :key #'mapped-class-of))
			  mapping-schema)))
    (make-instance 'foreign-key
		   :table table
		   :columns (mapcar #'(lambda (name)
					(get-column name table))
				    (columns-names-of one-to-many-definition))
		   :referenced-table (get-table
				      (table-name-of
				       (class-mapping-definition-of one-to-many-definition))
				      mapping-schema))))

(defgeneric compute-association (many-to-one-definition
				 one-to-many-definition
				 class-mapping-definitions
				 mapping-schema))

(defmethod compute-association ((many-to-one-definition many-to-one-mapping-definition)
				(one-to-many-definition (eql nil))
				class-mapping-definitions
				mapping-schema)
  (make-instance 'unidirectional-many-to-one-association
		 :foreign-key (compute-foreign-key many-to-one-definition
						   class-mapping-definitions
						   mapping-schema)
		 :many-to-one-definition many-to-one-definition
		 :mapping-schema mapping-schema))

(defmethod compute-association ((many-to-one-definition (eql nil))
				(one-to-many-definition one-to-many-mapping-definition)
				class-mapping-definitions
				mapping-schema)
  (make-instance 'unidirectional-one-to-many-association
		 :foreign-key (compute-foreign-key one-to-many-definition
						   class-mapping-definitions
						   mapping-schema)
		 :one-to-many-definition one-to-many-definition
		 :mapping-schema mapping-schema))

(defmethod compute-association ((many-to-one-definition many-to-one-mapping-definition)
				(one-to-many-definition one-to-many-mapping-definition)
				class-mapping-definitions
				mapping-schema)
  (make-instance 'bidirectional-association
		 :foreign-key (compute-foreign-key many-to-one-definition
						   class-mapping-definitions
						   mapping-schema)
		 :many-to-one-definition many-to-one-definition
		 :one-to-many-definition one-to-many-definition
		 :mapping-schema mapping-schema))

(defmethod initialize-instance :after ((instance unidirectional-many-to-one-association)
				       &key many-to-one-definition
				       mapping-schema)
  (with-slots (many-to-one-direction) instance
    (setf many-to-one-direction
	  (make-instance 'many-to-one-mapping
			 :association instance
			 :slot-name (slot-name-of many-to-one-definition)
			 :unmarshaller (unmarshaller-of many-to-one-definition)
			 :marshaller (marshaller-of many-to-one-definition)
			 :referenced-class-mapping (get-mapping
						    (mapped-class-of many-to-one-definition)
						    mapping-schema)))))

(defmethod initialize-instance :after ((instance unidirectional-one-to-many-association)
				       &key one-to-many-definition
				       mapping-schema)
  (with-slots (one-to-many-direction) instance
    (setf one-to-many-direction
	  (make-instance 'one-to-many-mapping
			 :association instance
			 :slot-name (slot-name-of one-to-many-definition)
			 :unmarshaller (unmarshaller-of one-to-many-definition)
			 :marshaller (marshaller-of one-to-many-definition)
			 :referenced-class-mapping (get-mapping
						    (mapped-class-of one-to-many-definition)
						    mapping-schema)))))

(defun compute-association-directions-table (class-mapping-definitions mapping-schema)
  (let ((association-directions-table (make-hash-table)))
    (loop for (many-to-one-definition . one-to-many-definition)
	 in (compute-reference-pairs class-mapping-definitions)
	 for association = (compute-association many-to-one-definition
						one-to-many-definition
						class-mapping-definitions
						mapping-schema)
       when (not (null many-to-one-definition))
       do (setf (gethash many-to-one-definition
			 association-directions-table)
		(many-to-one-direction-of association))
       when (not (null one-to-many-definition))
       do (setf (gethash one-to-many-definition
			 association-directions-table)
		(one-to-many-direction-of association))
       finally (return association-directions-table))))

(defun compute-superclasses-mappings (class-mapping-definition mapping-schema)
  (mapcar #'(lambda (class)
	      (get-mapping class mapping-schema))
	  (mapped-superclasses-of class-mapping-definition)))

(defun compute-subclasses (class-mapping-definition class-mapping-definitions)
  (let ((mapped-class (mapped-class-of class-mapping-definition)))
    (mapcar #'mapped-class-of
	    (remove-if-not #'(lambda (mapped-superclasses)
			       (not (null (find mapped-class mapped-superclasses))))
			   class-mapping-definitions
			   :key #'mapped-superclasses-of))))

(defun compute-association-foreign-keys (table association-directions-table)
  (loop for direction being the hash-values in association-directions-table
     for foreign-key = (foreign-key-of (association-of direction))
     when (eq (table-of foreign-key) table)
     collect foreign-key))

(defun compute-superclasses-foreign-keys (table superclasses-mappings)
  (mapcar #'(lambda (superclass-mapping)
	      (make-instance 'foreign-key
			     :table table
			     :columns (primary-key-of table)
			     :referenced-table (table-of superclass-mapping)))
	  superclasses-mappings))

(defun compute-value-mappings (class-mapping-definition table)
  (mapcar #'(lambda (definition)
	      (make-instance 'value-mapping
			     :columns (mapcar #'(lambda (name)
						  (get-column name table))
					      (columns-names-of definition))
			     :slot-name (slot-name-of definition)
			     :marshaller (marshaller-of definition)
			     :unmarshaller (unmarshaller-of definition)))
	  (value-mappings-of class-mapping-definition)))

(defmethod initialize-instance :after ((instance mapping-schema)
				       &key class-mapping-definitions)
  (with-slots (tables class-mappings) instance
    (setf tables (compute-tables class-mapping-definitions)
	  class-mappings (allocate-class-mappings class-mapping-definitions)))
  (let ((association-directions-table
	 (compute-association-directions-table class-mapping-definitions
					       instance)))
    (dolist (definition class-mapping-definitions instance)
      (let ((mapped-class (mapped-class-of definition))
	    (table (get-table (table-name-of definition) instance))
	    (superclasses-mappings (compute-superclasses-mappings definition instance)))
	(setf (slot-value table 'foreign-keys)
	      (append (compute-superclasses-foreign-keys table superclasses-mappings)
		      (compute-association-foreign-keys table
							association-directions-table)))
	(initialize-instance
	 (get-mapping mapped-class instance)
	 :superclasses-mappings superclasses-mappings
	 :subclasses-mappings (mapcar #'(lambda (mapped-subclass)
					  (get-mapping mapped-subclass instance))
				      (compute-subclasses definition
							  class-mapping-definitions))
	 :value-mappings (compute-value-mappings definition table)
	 :mapped-class mapped-class
	 :table table
	 :reference-mappings (append
			      (mapcar #'(lambda (many-to-one-definition)
					  (gethash many-to-one-definition
						   association-directions-table))
				      (many-to-one-mappings-of definition))
			      (mapcar #'(lambda (one-to-many-definition)
					  (gethash one-to-many-definition
						   association-directions-table))
				      (one-to-many-mappings-of definition))))))))

(defclass clos-session ()
  ((mappings :initarg :mappings :reader mappings-of)
   (connection :initarg :connection :reader connection-of)
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defun make-clos-session (connector mapping-schema)
  (make-instance 'clos-session
		 :connection (funcall connector)
		 :mapping-schema mapping-schema))

(defun get-class-mapping (session class)
  (multiple-value-bind (mapping present-p)
      (gethash class (mappings-of session))
    (if (not present-p)
	(error "Class ~a not mapped" class)
	mapping)))

(defclass joined-superclass ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (table-alias :initarg :table-alias
		:reader table-reference-of)
   (joined-superclasses :initarg :joined-superclasses
			:reader joined-superclasses-of)
   (joined-fetchings :initarg :joined-fetchings
		     :reader joined-fetchings-of)))

(defmethod initialize-instance :after ((instance joined-superclass)
				       &key class-mapping
				       (joined-superclasses
					(compute-joined-superclasses
					 (superclasses-mappings-of class-mapping))))
  (setf (slot-value instance 'joined-superclasses) joined-superclasses))

(defclass object-loader (joined-superclass)
  ((subclass-object-loaders :initarg :subclass-object-loaders
			    :reader subclass-object-loaders-of)))

(defun compute-joined-superclasses (superclasses-mappings)
  (mapcar #'(lambda (superclass-mapping)
	      (make-instance 'joined-superclass
			     :class-mapping superclass-mapping))
	  superclasses-mappings))

(defun compute-subclass-object-loaders (object-loader class-mapping)
  (mapcar #'(lambda (subclass-mapping)
	      (make-instance 'object-loader
			     :class-mapping subclass-mapping
			     :joined-superclasses (list* object-loader
							 (compute-joined-superclasses
							  (remove class-mapping
								  (superclasses-mappings-of subclass-mapping))))))
	  (subclasses-mappings-of class-mapping)))

(defmethod initialize-instance :after ((instance object-loader)
				       &key class-mapping)
  (setf (slot-value instance 'subclass-object-loaders)
	(compute-subclass-object-loaders instance class-mapping)))

(defun make-object-loader (class-mapping joined-fetchings)
  (declare (ignore joined-fetchings))
  (make-instance 'object-loader
		 :class-mapping class-mapping
		 :joined-superclasses (compute-joined-superclasses
				       (superclasses-mappings-of class-mapping))))

(defvar *session*)

(defun call-with-session (session function)
  (let ((*session* session))
    (funcall function)))

(defmacro with-session ((session) &body body)
  `(call-with-session ,session #'(lambda () ,@body)))