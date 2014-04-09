(in-package #:cl-db)

(defclass table ()
  ((name :initarg :name
	 :reader name-of)
   (columns :initarg :columns
	    :reader columns-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)))

(defclass column ()
  ((name :initarg :name
	 :reader name-of)
   (sql-type :initarg :sql-type
	     :reader sql-type-of)))

(defclass class-mapping ()
  ((table :reader table-of)
   (mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (value-mappings :initform nil
		   :reader value-mappings-of)
   (reference-mappings :initform nil
		       :reader reference-mappings-of)
   (extension-mappings :initarg :extension-mappings
		       :reader extension-mappings-of)
   (inheritance-mappings :initarg :inheritance-mappings
			 :reader inheritance-mappings-of)))

;;(defclass inheritance-mapping ()
;;  ((columns :initarg :columns
;;	    :reader columns-of)
;;   (subclass-mapping :initarg :subclass-mapping
;;		     :reader subclass-mapping-of)
;;   (superclass-mapping :initarg :superclass-mapping
;;		       :reader superclass-mapping-of)))

(defclass extension-mapping ()
  ((columns :initarg :columns
	    :reader columns-of)
   (subclass-mapping :initarg :subclass-mapping
		     :reader subclass-mapping-of)))

(defclass inheritance-mapping ()
  ((columns :initarg :columns
	    :reader columns-of)
   (superclass-mapping :initarg :superclass-mapping
		       :reader superclass-mapping-of)))

(defclass association ()
  ((columns :initarg :columns
	    :reader columns-of)
   (many-to-one-mapping :initarg :many-to-one-mapping
			:reader many-to-one-mapping-of)
   (one-to-many-mapping :initarg :one-to-many-mapping
			:reader one-to-many-mapping-of)))

(defclass slot-mapping ()
  ((path :initarg :path :reader path-of)
   (slot-name :initarg :slot-name :reader slot-name-of)
   (marshaller :initarg :marshaller :reader marshaller-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)))

(defclass value-mapping (slot-mapping)
  ((columns :initarg :columns :reader columns-of)))

(defclass reference-mapping (slot-mapping)
  ((association :initarg :association
		:reader association-of)
   (referenced-class-mapping :initarg :referenced-class-mapping
			     :reader referenced-class-mapping-of)))

(defclass many-to-one-mapping (reference-mapping)
  ())

(defclass one-to-many-mapping (reference-mapping)
  ())

(defclass mapping-schema ()
  ((tables :initarg :tables
	   :reader tables-of)
   (class-mappings :initarg :class-mappings
		   :reader class-mappings-of)
   (inheritance-mappings :initarg :inheritance-mappings
			 :reader inheritance-mappings-of)
   (association-mappings :initarg :association-mappings
			 :reader association-mappings-of)))

(defun get-slot-name (class reader)
  (let ((reader-name (generic-function-name reader)))
    (slot-definition-name
     (loop for class in (class-precedence-list class)
	thereis (find-if #'(lambda (direct-slot-definition)
			     (member reader-name
				     (slot-definition-readers
				      direct-slot-definition)))
			 (class-direct-slots class))
	finally (error "Slot for reader ~a not found" reader-name)))))

(defun get-value-mapping (class-mapping reader)
  (let* ((slot-name
	  (get-slot-name (mapped-class-of class-mapping) reader))
	 (value-mapping
	  (find slot-name (value-mappings-of class-mapping)
		:key #'slot-name-of)))
    (if (null value-mapping)
	(error "Mapped value for slot ~a not found" slot-name)
	value-mapping)))

(defun get-reference-mapping (class-mapping reader)
  (let ((reference-mapping
	 (find (get-slot-name (mapped-class-of class-mapping) reader)
	       (reference-mappings-of class-mapping)
	       :key #'slot-name-of)))
    (if (null reference-mapping)
	(error "Mapped reference for accessor ~a not found" reader)
	reference-mapping)))

(defvar *class-mapping-definitions*)

(defun find-class-mapping-definition (mapped-class &optional
				      (class-mapping-definitions
				       *class-mapping-definitions*))
  (gethash mapped-class class-mapping-definitions))

(defvar *mapping-schema*)

(defun get-class-mapping (mapped-class &optional
			  (mapping-schema *mapping-schema*))
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class (class-mappings-of mapping-schema))
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defun get-table (name &optional (mapping-schema *mapping-schema*))
  (multiple-value-bind (mapping present-p)
      (gethash name (tables-of mapping-schema))
    (if (not present-p)
	(error "Table ~a not found" name)
	mapping)))

(defun get-column (name table)
  (multiple-value-bind (column present-p)
      (gethash name (columns-of table))
    (if (not present-p)
	(error "Column ~a for table ~a not found" name (name-of table))
	column)))

(defun find-association (mapped-class referenced-class columns)
  (find (list mapped-class referenced-class columns)
	(association-mappings-of *mapping-schema*)
	:test #'equal
	:key #'(lambda (association)
		 (list (mapped-class-of
			(many-to-one-mapping-of association))
		       (mapped-class-of
			(one-to-many-mapping-of association))
		       (columns-of association)))))

(defun list-inhritance-mappings (&optional
				 (mapping-schema *mapping-schema*))
  (inheritance-mappings-of mapping-schema))

(defun value-mapping-p (slot-mapping-definition)
  (typep slot-mapping-definition 'value-mapping-definition))

(defun reference-mapping-p (slot-mapping-definition)
  (or (many-to-one-mapping-of slot-mapping-definition)
      (one-to-many-mapping-of slot-mapping-definition)))

(defun many-to-one-mapping-p (slot-mapping-definition)
  (typep slot-mapping-definition 'many-to-one-mapping-definition))

(defun one-to-many-mapping-p (slot-mapping-definition)
  (typep slot-mapping-definition 'one-to-many-mapping-definition))

(defgeneric find-column-type (column-name slot-mapping))

(defmethod find-column-type (column-name
			     (slot-mapping value-mapping-definition))
  (let ((column
	 (find column-name (columns-of slot-mapping)
	       :key #'column-name-of :test #'string=)))
    (when (not (null column))
      (column-type-of column))))

(defmethod find-column-type (column-name
			     (slot-mapping many-to-one-mapping-definition))
  (let ((column-position
	 (position column-name (columns-names-of slot-mapping))))
    (when (not (null column-position))
      (get-reference-column-type column-position
				 (referenced-class-of slot-mapping)))))

(defmethod find-column-type (column-name
			     (slot-mapping one-to-many-mapping-definition))
  (declare (ignore column-name slot-mapping))
  nil)

(defun get-column-type (column-name class-mapping-definition)
  (let ((column-type
	 (loop for slot-mapping-definition
	    in (value-mappings-of class-mapping-definition)
	    thereis (and (find-column-type column-name
					   slot-mapping-definition)))))
    (if (null column-type)
	(loop for slot-mapping-definition
	   in (many-to-one-mappings-of class-mapping-definition)
	   thereis (and (find-column-type column-name
					  slot-mapping-definition)))
	column-type)))

(defun get-reference-column-type (column-position mapped-class)
  (let* ((class-mapping-definition (find-class-mapping-definition mapped-class))
	 (primary-key (primary-key-of class-mapping-definition))
	 (superclasses (superclasses-of class-mapping-definition)))
    (if (not (null (or primary-key superclasses)))
	(if (not (null primary-key))
	    (get-column-type (elt primary-key column-position)
			     class-mapping-definition)
	    (get-reference-column-type column-position
				       (find-class-mapping-definition
					(first superclasses))))
	(error "For mapping of class ~a not specified primary key or superclasses"
	       mapped-class))))

(defun get-value-mapping-columns (class-mapping-definition)
  (loop for slot-mapping-definition
     in (value-mappings-of class-mapping-definition)
     append (mapcar #'(lambda (column-definition)
			(with-slots (column-name column-type)
			    column-definition
			  (make-instance 'column :name column-name
					 :sql-type column-type)))
		    (columns-of slot-mapping-definition))))

(defun get-many-to-one-columns (class-mapping-definition)
  (loop for slot-mapping-definition
     in (many-to-one-mappings-of class-mapping-definition)
     for columns = (columns-names-of slot-mapping-definition)
     append (mapcar #'(lambda (column-name)
			(make-instance 'column :name column-name
				       :sql-type
				       (get-reference-column-type
					(position column-name columns)
					(referenced-class-of slot-mapping-definition))))
		    columns)))
				 
(defun get-one-to-many-columns (class-mapping-definition &optional
				(class-mapping-definitions
				 (alexandria:hash-table-values
				  *class-mapping-definitions*)))
  (let ((mapped-class (mapped-class-of class-mapping-definition)))
    (loop for class-mapping-definition in class-mapping-definitions
       append
	 (loop for slot-mapping-definition
	    in (one-to-many-mappings-of class-mapping-definition)
	    for columns = (columns-names-of slot-mapping-definition)
	    when (eq mapped-class
		     (referenced-class-of slot-mapping-definition))
	    append (mapcar #'(lambda(column-name)
			       (make-instance 'column
					      :name column-name :sql-type
					      (get-reference-column-type
					       (position column-name columns)
					       (mapped-class-of class-mapping-definition))))
			   columns)))))

(defun get-primary-key-columns (class-mapping-definition)
  (mapcar #'(lambda (column-name)
	      (make-instance 'column :name column-name
			     :sql-type
			     (get-column-type column-name
					      class-mapping-definition)))
	  (primary-key-of class-mapping-definition)))

(defun get-superclasses-columns (class-mapping-definition)
  (loop for superclass in (superclasses-of class-mapping-definition)
     for superclass-mapping = (find-class-mapping-definition superclass)
     append (get-primary-key-columns superclass-mapping)))

(defun make-columns (class-mapping-definition)
  (alexandria:alist-hash-table
   (mapcar #'(lambda (column)
	       (cons (name-of column) column))
	   (remove-duplicates
	    (append
	     (get-value-mapping-columns class-mapping-definition)
	     (get-superclasses-columns class-mapping-definition)
	     (get-many-to-one-columns class-mapping-definition)
	     (get-one-to-many-columns class-mapping-definition))
	    :key #'name-of :test #'string=))
   :test #'equal))

(defun compute-tables (class-mapping-definitions)
  (mapcar #'(lambda (class-mapping-definition)
	      (make-instance 'table :name
			     (table-name-of class-mapping-definition)
			     :columns
			     (make-columns class-mapping-definition)
			     :primary-key
			     (primary-key-of class-mapping-definition)))
	    class-mapping-definitions))

(defun make-class-mappings (class-mapping-definitions)
  (mapcar #'(lambda (class-mapping-definition)
	      (make-instance 'class-mapping :mapped-class
			     (mapped-class-of class-mapping-definition)))
	  class-mapping-definitions))

(defun make-many-to-one-association (slot-mapping-definition
				     class-mapping-definition)
  (let ((table (get-table (table-name-of class-mapping-definition))))
    (make-instance 'association
		   :many-to-one-mapping
		   (get-class-mapping
		    (mapped-class-of class-mapping-definition))
		   :one-to-many-mapping
		   (get-class-mapping
		    (referenced-class-of slot-mapping-definition))
		   :columns (mapcar #'(lambda (column-name)
					(get-column column-name table))
				    (columns-names-of slot-mapping-definition)))))

(defun make-one-to-many-association (slot-mapping-definition
				     class-mapping-definition)
  (let* ((referenced-class
	  (referenced-class-of slot-mapping-definition))
	 (table
	  (get-table
	   (table-name-of
	    (find-class-mapping-definition referenced-class)))))
    (make-instance 'association
		   :many-to-one-mapping
		   (get-class-mapping referenced-class)
		   :one-to-many-mapping
		   (get-class-mapping
		    (mapped-class-of class-mapping-definition))
		   :columns (mapcar #'(lambda (column-name)
					(get-column column-name table))
				    (columns-names-of slot-mapping-definition)))))

(defun make-associations (class-mapping-definition)
  (append
   (loop for slot-mapping
      in (many-to-one-mappings-of class-mapping-definition)
      for association = (make-many-to-one-association slot-mapping
						      class-mapping-definition)
      when (not (null association))
      collect association)
   (loop for slot-mapping
      in (one-to-many-mappings-of class-mapping-definition)
      for association = (make-one-to-many-association slot-mapping
						      class-mapping-definition)
      when (not (null association))
      collect association)))

(defun compute-associations (class-mapping-definitions)
  (remove-duplicates
   (apply #'append (mapcar #'make-associations
			   class-mapping-definitions))
   :test #'equal
   :key (lambda (association)
	  (list (many-to-one-mapping-of association)
		(one-to-many-mapping-of association)
		(columns-of association)))))

(defun compute-mapping-presedence-list (mapped-class superclass)
  (list* superclass
	 (apply #'append
		(mapcar #'(lambda (superclass)
			    (when (eq mapped-class superclass)
			      (error "Superclass ~a inherit from subclass ~a"
				     superclass mapped-class))
			    (compute-mapping-presedence-list mapped-class
							     superclass)) 
			(superclasses-of
			 (find-class-mapping-definition superclass))))))

(defun class-mapping-presedence-list (class-mapping)
  (let ((mapped-class (mapped-class-of class-mapping)))
    (sort (compute-mapping-presedence-list mapped-class mapped-class)
	  #'string< :key #'(lambda (class)
			     (symbol-name (class-name class))))))

(defun symbol< (a b)
  (string< (symbol-name (class-name a))
	   (symbol-name (class-name b))))

(defun presedence-list< (a b)
  (let ((rest-a (rest a)))
    (if (and (not (null (symbol< (first a) (first b))))
	     (not (null rest-a)))
	(presedence-list< rest-a (rest b)))))

(defun compute-inheritance-mappings (class-mapping-definition)
  (mapcar #'(lambda (superclass)
	      (let ((table
		     (get-table
		      (table-name-of class-mapping-definition))))
		(make-instance 'inheritance-mapping
			       :columns (mapcar #'(lambda (name)
						    (cons name name))
						(primary-key-of (find-class-mapping-definition superclass)))
			       :superclass-mapping (get-class-mapping superclass))))
	  (superclasses-of class-mapping-definition)))

(defun compute-extension-mappings (class-mapping-definition)
  (let ((mapped-class (mapped-class-of class-mapping-definition)))
    (mapcar #'(lambda (subclass-mapping-definition)
		(let ((table
		       (get-table
			(table-name-of subclass-mapping-definition))))
		  (make-instance 'extension-mapping
				 :columns (mapcar #'(lambda (name)
						      (cons name name))
						  (primary-key-of class-mapping-definition))
				 :subclass-mapping (get-class-mapping
						    (mapped-class-of subclass-mapping-definition)))))
	    (remove-if #'(lambda (subclass-mapping-definition)
			   (not
			    (find mapped-class
				  (superclasses-of subclass-mapping-definition))))
		       (alexandria:hash-table-values *class-mapping-definitions*)))))

(defun compute-class-mapping (class-mapping class-mapping-definition)
  (with-slots (extension-mappings inheritance-mappings table)
      class-mapping
    (setf table
	  (get-table (table-name-of class-mapping-definition))
	  inheritance-mappings
	  (compute-inheritance-mappings class-mapping-definition)
	  extension-mappings
	  (compute-extension-mappings class-mapping-definition))))

(defun compute-value-mappings (class-mapping-definition path)
  (let ((table (get-table (table-name-of class-mapping-definition))))
    (loop for slot-mapping-definition
       in (value-mappings-of class-mapping-definition)
       collect (with-slots (slot-name columns marshaller unmarshaller)
		   slot-mapping-definition
		 (make-instance 'value-mapping 
				:slot-name slot-name
				:path path
				:columns (mapcar #'(lambda (column-definition)
						     (get-column
						      (column-name-of column-definition)
						      table))
						 columns)
				:marshaller marshaller
				:unmarshaller unmarshaller)))))

(defun compute-one-to-many-mapping (slot-mapping-definition path
				    class-mapping-definition)
  (with-slots (slot-name columns marshaller unmarshaller referenced-class)
      slot-mapping-definition
    (let ((table (get-table
		  (table-name-of
		   (find-class-mapping-definition referenced-class)))))
      (make-instance 'one-to-many-mapping
		     :slot-name slot-name
		     :path path
		     :association (find-association
				   referenced-class
				   (mapped-class-of class-mapping-definition)
				   (mapcar #'(lambda (name)
					       (get-column name table))
					   (columns-names-of slot-mapping-definition)))
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :referenced-class-mapping
		     (get-class-mapping referenced-class)))))

(defun compute-many-to-one-mapping (slot-mapping-definition path
				    class-mapping-definition)
  (let ((table (get-table (table-name-of class-mapping-definition))))
    (with-slots (slot-name columns marshaller unmarshaller referenced-class)
	slot-mapping-definition
      (make-instance 'many-to-one-mapping
		     :slot-name slot-name
		     :path path
		     :association (find-association
				   (mapped-class-of class-mapping-definition)
				   referenced-class
				   (mapcar #'(lambda (name)
					       (get-column name table))
					   (columns-names-of slot-mapping-definition)))
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :referenced-class-mapping
		     (get-class-mapping referenced-class)))))

(defun compute-reference-mappings (class-mapping-definition path)
  (append
   (loop for slot-mapping-definition
      in (many-to-one-mappings-of class-mapping-definition)
      collect (compute-many-to-one-mapping slot-mapping-definition path
					   class-mapping-definition))
   (loop for slot-mapping-definition
      in (one-to-many-mappings-of class-mapping-definition)
      collect (compute-one-to-many-mapping slot-mapping-definition path
					   class-mapping-definition))))
;; value-mappings reference-mappings - from list to hash-table 
(defun compute-slot-mappings (class-mapping &optional
			      (class-mapping-definition
			       (find-class-mapping-definition
				(mapped-class-of class-mapping))) path)
  (with-slots (value-mappings reference-mappings)
      class-mapping
    (setf value-mappings
	  (append value-mappings
		  (compute-value-mappings class-mapping-definition path))
	  reference-mappings
	  (append reference-mappings
		  (compute-reference-mappings class-mapping-definition path))))
  (dolist (inheritance-mapping (inheritance-mappings-of 
				(get-class-mapping
				 (mapped-class-of class-mapping-definition))))
    (compute-slot-mappings class-mapping
			   (find-class-mapping-definition
			    (mapped-class-of
			     (superclass-mapping-of inheritance-mapping)))
			   (list* inheritance-mapping path))))

(defun make-mapping-schema (name)
  (let* ((class-mapping-definitions
	  (gethash name *mapping-schema-definitions*))
	 (*class-mapping-definitions* class-mapping-definitions)
	 (tables
	  (compute-tables
	   (alexandria:hash-table-values class-mapping-definitions)))
	 (class-mappings
	  (make-class-mappings
	   (alexandria:hash-table-values class-mapping-definitions)))
	 (*mapping-schema*
	  (make-instance 'mapping-schema
			 :tables (alexandria:alist-hash-table
				  (mapcar #'(lambda (table)
					      (cons (name-of table)
						    table))
					  tables))
			 :class-mappings (alexandria:alist-hash-table
					  (mapcar #'(lambda (class-mapping)
						      (cons
						       (mapped-class-of class-mapping)
						       class-mapping))
						  class-mappings)))))
    (with-slots (association-mappings inheritance-mappings)
	*mapping-schema*
      (setf association-mappings
	    (compute-associations
	     (alexandria:hash-table-values class-mapping-definitions))
;;	    inheritance-mappings
;;	    (compute-inheritance-mappings
;;	     (alexandria:hash-table-values class-mapping-definitions))
	    ))
    (dolist (class-mapping
	      (sort class-mappings #'presedence-list<
		    :key #'class-mapping-presedence-list)
	     *mapping-schema*)
      (compute-class-mapping class-mapping
			     (find-class-mapping-definition
			      (mapped-class-of class-mapping))))
    (dolist (class-mapping class-mappings *mapping-schema*)
      (compute-slot-mappings class-mapping))))