;;; cl-db.lisp

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

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (superclasses :initarg :superclasses
		 :reader superclasses-of)
   (slot-mappings :initarg :slot-mappings
		  :reader slot-mappings-of)))

(defclass slot-mapping-definition ()
  ((slot-name :initarg :slot-name
	      :reader slot-name-of)
   (unmarshaller :initarg :unmarshaller
		 :reader unmarshaller-of)
   (marshaller :initarg :marshaller
	       :reader marshaller-of)))

(defclass value-mapping-definition
    (slot-mapping-definition)
  ((columns :initarg :columns
	    :reader columns-of)))

(defclass reference-mapping-definition
    (slot-mapping-definition)
  ((columns-names :initarg :columns-names
		  :reader columns-names-of)
   (referenced-class :initarg :referenced-class
		     :reader referenced-class-of)))

(defclass many-to-one-mapping-definition
    (reference-mapping-definition)
  ())

(defclass one-to-many-mapping-definition
    (reference-mapping-definition)
  ())

(defclass class-mapping ()
  ((table :reader table-of)
   (mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (value-mappings :reader value-mappings-of)
   (reference-mappings :reader reference-mappings-of)
   (subclasses-inheritance :reader subclasses-inheritance-of)
   (superclasses-inhertance :reader superclasses-inheritance-of)))

(defclass inheritance-mapping ()
  ((columns :initarg :columns
	    :reader columns-of)
   (subclass-mapping :initarg :subclass-mapping
		     :reader subclass-mapping-of)
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
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

(defclass many-to-one-mapping (reference-mapping) ())

(defclass one-to-many-mapping (reference-mapping) ())

(defclass mapping-schema ()
  ((tables :initarg :tables
	   :reader tables-of)
   (associations :initarg :associations
		 :reader associations-of)
   (class-mappings :initarg :class-mappings
		   :reader class-mappings-of)))

(defun get-slot-name (class reader)
  (let ((reader-name (generic-function-name reader)))
    (loop for slot in (class-slots class)
       when (find reader-name (slot-definition-readers slot))
       return (slot-definition-name slot)
       finally (error "Slot for reader ~a not found" reader-name))))

(defvar *class-mapping-definitions*)

(defun find-class-mapping-definition (mapped-class &optional
				      (class-mapping-definitions
				       *class-mapping-definitions*))
  (find mapped-class class-mapping-definitions :key #'mapped-class-of))

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
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defgeneric find-column-type (column-name slot-mapping))

(defmethod find-column-type (column-name
			     (slot-mapping value-mapping-definition))
  (let ((column (assoc column-name (columns-of slot-mapping)
		       :test #'equal)))
    (when (not (null column))
      (sql-type-of column))))

(defmethod find-column-type (column-name
			     (slot-mapping many-to-one-mapping-definition))
  (let ((column-position (position column-name (columns-of slot-mapping))))
    (when (not (null column-position))
      (get-reference-column-type column-position
				 (mapped-class-of slot-mapping)))))

(defmethod find-column-type (column-name
			     (slot-mapping one-to-many-mapping-definition))
  (declare (ignore column-name slot-mapping))
  nil)

(defun get-column-type (column-name class-mapping-definition)
  (let ((column-type (loop for value-mapping
			in (value-mappings-of class-mapping-definition)
			thereis (find-column-type column-name value-mapping))))
    (if (null column-type)
	(loop for many-to-one-mapping
	   in (many-to-one-mappings-of class-mapping-definition)
	   thereis (find-column-type column-name many-to-one-mapping))
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
  (loop for value-mapping
     in (value-mappings-of class-mapping-definition)
     append (columns-of value-mapping)))

(defun get-many-to-one-columns (class-mapping-definition)
  (loop for many-to-one-mapping
     in (many-to-one-mappings-of class-mapping-definition)
     for columns = (columns-of many-to-one-mapping)
     append (loop for column-name in columns collect
	       (make-instance 'column :name column-name
			      :sql-type
			      (get-reference-column-type
			       (position column-name columns)
			       (mapped-class-of many-to-one-mapping))))))
				 
(defun get-one-to-many-columns (class-mapping-definition
				&optional (class-mapping-definitions
					   *class-mapping-definitions*))
  (let ((mapped-class (mapped-class-of class-mapping-definition)))
    (loop for class-mapping-definition in class-mapping-definitions
       append
	 (loop for one-to-many
	    in (one-to-many-mappings-of class-mapping-definition)
	    for columns = (columns-of one-to-many)
	    when (eq mapped-class (mapped-class-of one-to-many))
	    append (loop for column-name in columns collect
			(make-instance 'column
				       :name column-name :sql-type
				       (get-reference-column-type
					(position column-name columns)
					class-mapping-definition)))))))

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
  (remove-duplicates
   (append
    (get-value-mapping-columns class-mapping-definition)
    (get-superclasses-columns class-mapping-definition)
    (get-many-to-one-columns class-mapping-definition)
    (get-one-to-many-columns class-mapping-definition)
    :key #'name-of :test #'string=)))

(defun make-tables (class-mapping-definitions)
    (mapcar #'(lambda (class-mapping-definition)
		(make-instance 'table :name
			       (table-name-of class-mapping-definition)
			       :columns (make-columns class-mapping-definition)))
	    class-mapping-definitions))

(defun make-class-mappings (class-mapping-definitions)
  (mapcar #'(lambda (class-mapping-definition)
	      (make-instance 'class-mapping :mapped-class
			     (mapped-class-of class-mapping-definition)))
	  class-mapping-definitions))

(defun make-reference-definition-pairs (class-mapping-definitions)
  (loop for class-mapping-definition in class-mapping-definitions
     append
       (loop for many-to-one-definition
	  in (many-to-one-mappings-of class-mapping-definition)
	  collect
	    (cons many-to-one-definition
		  (find-one-to-many-definition many-to-one-definition)))
       (loop for one-to-many-definition
	  in (one-to-many-mappings-of class-mapping-definition)
	  collect (cons one-to-many-definition))))

(defgeneric make-association (slot-mapping-definition class-mapping-definition))

(defmethod make-asociation ((slot-mapping-definition value-mapping-definition)
			    class-mapping-definition)
  (declare (ignore slot-mapping-definition class-mapping-definition))
  nil)

(defmethod make-association ((slot-mapping-definition many-to-one-definition)
			     class-mapping-definition)
  (let* ((many-to-one (get-class-mapping
		       (mapped-class-of class-mapping-definition)))
	 (many-to-one-table (table-of many-to-one)))
    (make-instance 'association
		   :many-to-one many-to-one
		   :one-to-many (get-class-mapping
				 (mapped-class-of slot-mapping-definition))
		   :columns (mapcar #'(lambda (column-name)
					(get-column column-name table))
				    (columns-of slot-mapping-definition)))))

(defmethod make-association ((slot-mapping-definition one-to-many-definition)
			     class-mapping-definition)
  (let* ((many-to-one (get-class-mapping
		       (mapped-class-of slot-mapping-definition)))
	 (table (table-of many-to-one)))
    (make-instance 'association
		   :many-to-one many-to-one
		   :one-to-many (get-class-mapping
				 (mapped-class-of class-mapping-definition))
		   :columns (mapcar #'(lambda (column-name)
					(get-column column-name table))
				    (columns-of slot-mapping-definition)))))

(defun make-associations (class-mapping-definition)
  (loop for slot-mapping
     in (slot-mappings-of class-mapping-definition)
     for association = (make-association slot-mapping class-mapping-definition)
     when (not (null association))
     collect association))

(defun compute-associations (class-mapping-definitions)
  (remove-duplicates
   (mapcar #'make-associations class-mapping-definitions)
   :test #'equal
   :key #'(lambda (association)
	    (list (many-to-one-of association)
		  (one-to-many-of association)
		  (columns-of association)))))

(defun compute-mapping-presedence-list (mapped-class superclass)
  (list* superclass
	 (mapcar #'(lambda (superclass)
		     (when (eq mapped-class superclass)
		       (error "Superclass ~a inherit from subclass ~a"
			      superclass mapped-class))
		     (compute-mapping-presedence-list mapped-class
						      superclass)) 
		 (superclasses-of
		  (find-class-mapping-definition superclass)))))

(defun mapping-presedence-list (class-mapping)
  (let ((mapped-class (mapped-class-of class-mapping)))
    (sort (compute-mapping-presedence-list mapped-class mapped-class)
	  #'string< :key #'symbol-name)))

(defun symbol< (a b)
  (string< (symbol-name a) (symbol-name b)))

(defun presedence-list< (a b predicate)
  (if (not (null (symbol< (first a) (first b))))
      (list< (rest a) (rest b) predicate)))

(defun 
  (mapcar #'(lambda (superclass)
	      (make-instance 'inheritance-mapping
			     :superclass-mapping (get-class-mapping superclass)
			     :subclass-mapping 
  (with-accessors ((superclasses superclasses-of))
      (definition-of class-mapping)
    (loop for superclass-name in superclasses
       for superclass-mapping = (find (find-class superclass-name)
				      class-mappings
				      :key #'mapped-class-of)
       do (push superclass-mapping
		(superclasses-inheritance-of class-mapping))
       do (push class-mapping
		(subclasses-inheritance-of superclass-mapping)))))

(defun compute-inheritance-mapping (class-mapping class-mappings)
  (with-accessors ((superclasses superclasses-of))
      (definition-of class-mapping)
    (loop for superclass-name in superclasses
       for superclass-mapping = (find (find-class superclass-name)
				      class-mappings
				      :key #'mapped-class-of)
       do (push superclass-mapping
		(superclasses-inheritance-of class-mapping))
       do (push class-mapping
		(subclasses-inheritance-of superclass-mapping)))))

(defun compute-inheritance-mappings (class-mapping-definitions)
  (append (mapcar #'compute-class-mapping-inheritance-mappings
		  class-mapping-definitions)))

(defun compute-value-mappings (class-mapping-definition
			       &optional (path (list)))
  (let ((class-mapping 
	(table (get-table (table-name-of class-mapping-definition))))
    (apply #'append
	   (mapcar #'(lambda (value-mapping-definition)
		       (with-slots (slot-name
				    columns marshaller unmarshaller)
			   value-mapping-definition
			 (make-instance 'value-mapping 
					:slot-name slot-name
					:path path
					:columns (mapcar #'(lambda (column-definition)
							     (get-column
							      (column-name-of column-definition)
							      table))
							 columns)
					:marshaller marshaller
					:unmarshaller unmarshaller)))
		   (value-mappings-of class-mapping-definition))
	   (mapcar #'(lambda (superclass-name)
		       (compute-value-mappings
			(superclass-mapping-of inhertance-mapping)
		      (list* inheritance-mapping path)))
		 (superclasses-inheritance-of class-mapping))))

(defun compute-class-mapping (class-mapping class-mapping-definition)
  (with-slots (table value-mappings reference-mappings
	       subclass-inheritance-mappings
	       superclasses-inheritance-mappings)
      class-mapping
    (setf table
	  (find-table (table-name-of class-mapping-definition))
	  superclasses-inheritance-mappings
	  (compute-superclasses-inheritance-mapping class-mapping-definition)
	  subclass-inheritance-mappings
	  (compute-subclass-inheritance-mappings class-mapping-definition)
	  value-mappings
	  (compute-value-mappings class-mapping-definition)
	  reference-mappings
	  (compute-reference-mappings class-mapping-definition))))

;; сначала созадем таблицы
;; затем создаем, но не инициализируем отображения
;; создаем асциации
;; теперь есть все, чтобы проинициализировать отображения
;; инициализируем в порядке наследования (presedence-list)

(defun make-mapping-schema (&rest class-mapping-definitions)
  (let* ((*class-mapping-definitions* class-mapping-definitions)
	 (tables
	  (compute-tables class-mapping-definitions))
	 (class-mappings
	  (make-class-mappings class-mapping-definitions))
	 (*mapping-schema*
	  (make-instance 'mapping-schema
			 :tables (alexandria:alist-hash-table
				  (mapcar #'(lambda (class-mapping)
					      (let ((table (table-of class-mapping)))
						(cons (name-of table)
						      table)))
					  tables))
			 :class-mappings (alexandria:alist-hash-table
					  (mapcar #'(lambda (class-mapping)
						      (cons
						       (mapped-class-of class-mapping)
						       class-mapping))
						  class-mappings)))))

    (with-slots (association-mappings inheritance-mappings)
	*mapping-schema*
      (setf association-mappingss
	    (compute-associations class-mapping-definitions)
	    inheritance-mappings
	    (compute-iheritance-mappings class-mapping-definitions)))
    (dolist (class-mapping
	      (sort class-mappings #'presedence-list<
		    :key #'class-mapping-presedence-list)
	     mapping-schema)
      (compute-class-mapping class-mapping
			     (find-class-mapping-definition
			      (mapped-class-of class-mapping))))))

;;;;;;;;;





(defun get-reference-mapping (class-mapping reference-reader)
  (multiple-value-bind (reference-mapping presentp)
      (gethash
       (get-slot-name (mapped-class-of class-mapping)
		      reference-reader)
       (reference-mappings-of class-mapping))
    (if (not presentp)
	(error "Mapped reference for accessor ~a not found"
	       reference-reader)
	reference-mapping)))









(defgeneric make-slot-mapping (slot-mapping-definition &rest path))

(defmethod make-slot-mapping ((definition value-mapping-definition)
			      &rest path)
  (with-accessors ((slot-name slot-name-of)
		   (columns columns-of)
		   (marshaller marshaller-of)
		   (unmarshaller unmarshaller-of))
      definition
    (make-instance 'value-mapping
		   :slot-name slot-name
		   :columns columns
		   :marshaller marshaller
		   :unmarshaller unmarshaller
		   :path path)))

(defmethod make-slot-mapping ((definition many-to-one-definition) path)
  (with-accessors ((slot-name slot-name-of)
		   (columns columns-of)
		   (marshaller marshaller-of)
		   (unmarshaller unmarshaller-of)
		   (mapped-class mapped-class-of))
      definition
    (make-instance 'many-to-one-mapping
		   :slot-name slot-name
		   :class-mapping (get-class-mapping mapped-class)
		   :columns columns
		   :marshaller marshaller
		   :unmarshaller unmarshaller
		   :path path)))

(defmethod make-slot-mapping ((definition one-to-many-definition) path)
  (with-accessors ((slot-name slot-name-of)
		   (columns columns-of)
		   (marshaller marshaller-of)
		   (unmarshaller unmarshaller-of)
		   (mapped-class mapped-class-of))
      definition
    (make-instance 'one-to-many-mapping
		   :slot-name slot-name
		   :class-mapping (get-class-mapping mapped-class)
		   :columns columns
		   :marshaller marshaller
		   :unmarshaller unmarshaller
		   :path path)))

(defun compute-reference-mappings (class-mapping &rest path)
  (let ((definition (definition-of class-mapping)))
    (apply #'append
	   (mapcar #'(lambda (reference-mapping-definition)
		       (make-slot-mapping reference-mapping-definition path))
		   (many-to-one-mappings-of definition))
	   (mapcar #'(lambda (reference-mapping-definition)
		       (make-slot-mapping reference-mapping-definition path))
		   (one-to-many-mappings-of definition))
	   (mapcar #'(lambda (inheritance-mapping)
		       (compute-value-mappings
			(superclass-mapping-of inhertance-mapping)
			(list* inheritance-mapping path)))
		   (superclasses-inheritance-of class-mapping)))))
    

(defun get-one-to-many-mapping (mapped-class many-to-one-mapping)
  (let ((columns (columns-of many-to-one-mapping)))
    (find-if #'(lambda (one-to-many-mapping)
		 (and (eq (mapped-class-of one-to-many-mapping)
			  mapped-class)
		      (equal (columns-of one-to-many-mapping)
			     columns)))
	     (one-to-many-mappings-of
	      (reference-class-mapping many-to-one-mapping)))))

(defun bidirectional-p (mapped-class many-to-one-mapping)
  (not (null (get-one-to-many-mapping mapped-class
				      many-to-one-mapping))))

(defun get-many-to-one-foreign-keys (class-mapping-definition)
  (loop for many-to-one-mapping
     in (many-to-one-mappings-of class-mapping-definition)
     when (not (bidirectional-p
		(mapped-class-of class-mapping-definition)
		many-to-one-mapping))
       collect (let ((reference-class-mapping
		     (reference-class-mapping many-to-one-mapping)))
		`(,(make-many-to-one-foreign-key-name
		    class-mapping-definition
		    many-to-one-mapping)
		   ,(table-symbol class-mapping-definition)
		   ,(table-symbol reference-class-mapping)
		   ,@(columns-of many-to-one-mapping)))))

(defun get-one-to-many-foreign-keys (class-mapping-definition)
  (loop for one-to-many-mapping
     in (one-to-many-mappings-of class-mapping-definition)
     collect (let ((reference-class-mapping
		    (reference-class-mapping one-to-many-mapping)))
	       `(,(make-one-to-many-foreign-key-name
		   class-mapping-definition
		   one-to-many-mapping)
		  ,(table-symbol reference-class-mapping)
		  ,(table-symbol class-mapping-definition)
		  ,@(columns-of one-to-many-mapping)))))

(defun get-reference-foreign-keys (class-mapping-definition)
  (append
   (get-one-to-many-foreign-keys class-mapping-definition)
   (get-many-to-one-foreign-keys class-mapping-definition)))

(defun compile-reference-foreign-keys (&rest class-mapping-definitions)
  (mapcar #'(lambda (foreign-key)
	      (apply #'compile-foreign-key foreign-key))
	  (loop for class-mapping-definition
	     in class-mapping-definitions append
	       (get-reference-foreign-keys class-mapping-definition))))

(defun compile-value-mappings (class-mapping-definition)
  (mapcar #'(lambda (value-mapping)
	      (with-slots (slot-name columns marshaller unmarshaller)
		  value-mapping
	      `(make-instance 'value-mapping
			      :slot-name (quote ,slot-name)
			      :marshaller ,marshaller
			      :unmarshaller ,unmarshaller
			      :columns (quote ,(mapcar #'first
						      columns)))))
	  (value-mappings-of class-mapping-definition)))

(defun get-class-mapping-name (mapped-class)
  (alexandria:ensure-symbol
   (lisp-name (format nil "~a-mapping" mapped-class))))

(defun compile-reference-mapping (mapping-type fk-name mapping)
  (with-slots (slot-name columns mapped-class marshaller unmarshaller)
      mapping
    `(cons (quote ,slot-name)
	   (make-instance (quote ,mapping-type)
			  :slot-name (quote ,slot-name)
			  :class-mapping ,(get-class-mapping-name mapped-class)
			  :foreign-key ,fk-name
			  :marshaller ,marshaller
			  :unmarshaller ,unmarshaller))))

(defun compile-superclass-mappings (subclass-mapping-definition)
  `((superclasses-inheritance-of
     ,(get-class-mapping-name
       (mapped-class-of subclass-mapping-definition)))
    (list ,@(mapcar #'(lambda (mapped-class)
			`(make-instance 'superclass-mapping
					:class-mapping ,(get-class-mapping-name
							 mapped-class)
					:foreign-key ,(make-foreign-key-symbol
						       (make-superclass-foreign-key-name
							subclass-mapping-definition
							(find-class-mapping-definition mapped-class)))))
		    (superclasses-of subclass-mapping-definition)))))

(defun compile-subclass-mappings (superclass-mapping-definition)
  (let ((class-name
	 (mapped-class-of superclass-mapping-definition)))
    `((subclasses-inheritance-of
       ,(get-class-mapping-name class-name))
      (list ,@(loop for class-mapping-definition in (list-class-mappings)
		 when (not
		       (null (find class-name
				   (superclasses-of class-mapping-definition))))
		 collect
		   `(make-instance 'subclass-mapping
				   :class-mapping ,(get-class-mapping-name
						    (mapped-class-of class-mapping-definition))
				   :foreign-key ,(make-foreign-key-symbol
						  (make-superclass-foreign-key-name
						   class-mapping-definition
						   superclass-mapping-definition))))))))

(defun make-class-mapping (class-mapping-definition)
  (with-slots (mapped-class) class-mapping-definition
    `(,(get-class-mapping-name mapped-class)
       (make-instance 'class-mapping
		      :mapped-class (find-class
				     (quote ,mapped-class))
		      :table ,(table-name-of class-mapping-definition)
		      :value-mappings (list
				       ,@(compile-value-mappings
					  class-mapping-definition))))))

(defgeneric make-association (many-to-one-definition
			      one-to-many-definition))

(defmethod make-association ((many-to-one-definition many-to-one-definition))
  (let ((many-to-one-table
	 (table-of
	  (get-class-mapping
	   (mapped-class-of
	    (class-mapping-definition-of many-to-one-definition)))))
	(one-to-many-table
	 (table-of
	  (get-class-mapping
	   (mapped-class-of many-to-one-definition))))
	(foreign-key
	 (make-instance 'foreign-key
			:table many-to-one-table
			:reference-table one-to-many-table
			:columns (columns-of many-to-one-definition))))
    (make-instance 'association :foreign-key foreign-key)))

(defun make-foreign-key (table referenced-table columns)
  (make-instance 'foreign-key
		 :table table
		 :columns columns
		 :reference-table referenced-table))

(defmethod make-association ((one-to-many-definition one-to-many-definition))
  (let ((many-to-one-table
	 (table-of
	  (get-class-mapping
		    (mapped-class-of one-to-many-definition))))
	(one-to-many-table
	 (table-of
	  (get-class-mapping
		    (mapped-class-of one-to-many-definition))))
	(foreign-key
	 (make-foreign-key many-to-one-table` one-to-many-table
			   (columns-of one-to-many-definition))))
    (make-instance 'association :foreign-key foreign-key)))

(defun make-reference-definition-pairs (class-mapping-definitions)
  (loop for class-mapping-definition in class-mapping-definitions
     append
       (loop for many-to-one-definition
	  in (many-to-one-mappings-of class-mapping-definition)
	  collect
	    (cons many-to-one-definition
		  (find-one-to-many-definition many-to-one-definition)))
       (loop for one-to-many-definition
	  in (one-to-many-mappings-of class-mapping-definition)
	  collect (cons one-to-many-definition))))

(defun make-associations (class-mapping-definitions)
  (loop for (many-to-one-definition . one-to-many-definition)
     collect (if (not (null many-to-one-definition))
		 (make-association many-to-one-definition)
		 (make-association one-to-many-definition))))