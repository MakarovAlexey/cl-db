;;; cl-db.lisp

(in-package #:cl-db)

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (value-mappings :initarg :value-mappings
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)))

(defclass root-class-mapping-definition
    (class-mapping-definition)
  ((primary-key :initarg :primary-key
		:reader primary-key-of)))

(defclass subclass-mapping-definition
    (class-mapping-definition)
  ((mapped-superclasses :initarg :mapped-superclasses
			:reader mapped-superclasses-of)))

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

(defun map-class (class-name table-name primary-key &rest slots)
  (make-instance 'root-class-mapping-definition
		 :table-name table-name
		 :primary-key primary-key
		 :mapped-class (find-class class-name)
		 :value-mappings (remove-if-not #'value-mapping-p slots)
		 :many-to-one-mappings (remove-if-not #'many-to-one-mapping-p slots)
		 :one-to-many-mappings (remove-if-not #'one-to-many-mapping-p slots)))

(defun map-subclass (class-name superclasses table-name &rest slots)
  (make-instance 'subclass-mapping-definition
		 :table-name table-name
		 :mapped-class (find-class class-name)
		 :mapped-superclasses (mapcar #'find-class superclasses)
		 :value-mappings (remove-if-not #'value-mapping-p slots)
		 :many-to-one-mappings (remove-if-not #'many-to-one-mapping-p slots)
		 :one-to-many-mappings (remove-if-not #'one-to-many-mapping-p slots)))

(defclass slot-mapping-definition ()
  ((class-mapping-definition :accessor class-mapping-definition-of)
   (slot-name :initarg :slot-name :reader slot-name-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defclass value-mapping-definition (slot-mapping-definition)
  ((columns-definitions :initarg :columns-definitions
			:reader columns-definitions-of)))

(defclass reference-mapping-definition (slot-mapping-definition)
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defclass one-to-many-mapping-definition
    (reference-mapping-definition)
  ())

(defclass many-to-one-mapping-definition
    (reference-mapping-definition)
 ())

(defun map-slot (slot-name mapping
		 &optional (marshaller #'list) (unmarshaller #'list))
  (funcall mapping slot-name marshaller unmarshaller))

(defclass column-definition ()
  ((name :initarg :name :reader name-of)
   (type-name :initarg :type-name :reader type-name-of)))

(defun column (name type-name)
  (make-instance 'column-definition :name name :type-name type-name))

(defun value (column &rest columns)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'value-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :columns-definitions (list* column columns))))

(defun one-to-many (class-name column-name &rest column-names)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'one-to-many-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :mapped-class (find-class class-name)
		     :columns-names (list* column-name column-names))))

(defun many-to-one (class-name column-name &rest column-names)
  #'(lambda (slot-name marshaller unmarshaller)
      (make-instance 'many-to-one-mapping-definition
		     :slot-name slot-name
		     :marshaller marshaller
		     :unmarshaller unmarshaller
		     :mapped-class (find-class class-name)
		     :columns-names (list* column-name column-names))))

(defclass mapping-schema ()
  ((class-mapping-definitions :initarg :class-mapping-definitions
			      :reader class-mapping-definitions-of)
   (class-mappings :initform (make-hash-table)
		   :reader class-mappings-of)
   (tables :initform (make-hash-table)
	   :reader tables-of)))

(defvar *mapping-schema*)

(defun find-class-mapping-definition (mapped-class)
  (find mapped-class (class-mapping-definitions-of *mapping-schema*)
	:key #'mapped-class-of))

(defun get-table (name mapping-schema)
  (gethash name (tables-of mapping-schema)))

(defun get-mapping (mapped-class mapping-schema)
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class (class-mappings-of mapping-schema))
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defclass class-mapping ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table :initarg :table
	  :reader table-of)
   (value-mappings :initarg :value-mappings
		   :reader value-mappings-of)
   (reference-mappings :initform (list)
		       :reader reference-mappings-of)
   (subclasses-mappings :initform (list)
			:reader subclasses-mappings-of)
   (superclasses-mappings :initform (list)
			  :reader superclasses-mappings-of)))

(defclass foreign-key-mapping ()
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass inheritance-mapping (foreign-key-mapping)
  ((subclass-mapping :initarg :subclass-mapping
		     :reader subclass-mapping-of)
   (superclass-mapping :initarg :superclass-mapping
		       :reader superclass-mapping-of)))

(defclass association (foreign-key-mapping)
  ())

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
  ((association :initarg :association
		:reader association-of)
   (referenced-class-mapping :initarg :referenced-class-mapping
			     :reader referenced-class-mapping-of)))

(defclass one-to-many-mapping (reference-mapping) ())

(defclass many-to-one-mapping (reference-mapping) ())

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initform (make-hash-table) :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initform (list) :reader foreign-keys-of)))

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

(defmethod initialize-instance :after ((instance foreign-key) &key table)
  (with-slots (foreign-keys) table
    (push instance foreign-keys)))

(defun compute-tables (class-mapping-definitions)
  (let ((tables (make-hash-table :test #'equal
				 :size (length class-mapping-definitions))))
    (dolist (class-mapping-definition class-mapping-definitions tables)
      (let ((table-name (table-name-of class-mapping-definition)))
	(setf (gethash table-name tables)
	      (make-instance 'table :name table-name))))))

(defun compute-class-mappings (class-mapping-definitions mapping-schema)
  (let ((class-mappings (make-hash-table :size (length class-mapping-definitions))))
    (dolist (class-mapping-definition class-mapping-definitions class-mappings)
      (let ((mapped-class (mapped-class-of class-mapping-definition)))
	(setf (gethash mapped-class class-mappings)
	      (make-instance 'class-mapping
			     :mapped-class mapped-class
			     :table (get-table
				     (table-name-of class-mapping-definition)
				     mapping-schema)))))))

(defun add-column (column-definition table)
  (let ((name (name-of column-definition))
	(type-name (type-name-of column-definition)))
    (multiple-value-bind (column presentp)
	(gethash name (columns-of table))
      (declare (ignore column))
      (when presentp
	(error "Column '~a' in table '~a' already exist"
	       name (name-of table)))
      (setf (gethash name (columns-of table))
	    (make-instance 'column :name name
			   :type-name type-name)))))

(defun compute-value-mappings (class-mapping-definition mapping-schema)
  (let* ((class-mapping (get-mapping
			 (mapped-class-of class-mapping-definition)
			 mapping-schema))
	 (table (table-of class-mapping)))
    (with-slots (value-mappings) class-mapping
      (setf value-mappings
	    (mapcar #'(lambda (definition)
			(make-instance 'value-mapping
				       :columns (mapcar #'(lambda (column-definition)
							    (add-column column-definition table))
							(columns-definitions-of definition))
				       :slot-name (slot-name-of definition)
				       :marshaller (marshaller-of definition)
				       :unmarshaller (unmarshaller-of definition)))
		    (value-mappings-of class-mapping-definition))))))

(defun compute-reference-pairs (class-mapping-definitions)
  (let ((many-to-one-definitions
	 (apply #'append (mapcar #'many-to-one-mappings-of
				 class-mapping-definitions)))
	(one-to-many-definitions
	 (apply #'append (mapcar #'one-to-many-mappings-of
				 class-mapping-definitions)))
	(associations (list)))
    (dolist (many-to-one-definition many-to-one-definitions)
      (setf associations
	    (acons many-to-one-definition
		   (find-if #'(lambda (one-to-many-definition)
				(and (eq (mapped-class-of one-to-many-definition)
					 (mapped-class-of 
					  (class-mapping-definition-of many-to-one-definition)))
				     (not (set-difference (columns-names-of one-to-many-definition)
							  (columns-names-of many-to-one-definition)
							  :test #'string=))))
			    one-to-many-definitions)
		   associations)))
    (dolist (one-to-many-definition one-to-many-definitions)
      (when (null (rassoc one-to-many-definition associations))
	(setf associations
	      (acons nil one-to-many-definition associations))))
    associations))

(defun get-column-definition (column-name value-mapping-definition)
  (find column-name (columns-definitions-of value-mapping-definition)
	:key #'name-of))

(defun find-value-column-definition (column-name
				     class-mapping-definition)
  (let ((value-mapping-definition
	 (find-if #'(lambda (value-mapping-definition)
		      (find column-name
			    (columns-definitions-of value-mapping-definition)
			    :key #'name-of))
		  (value-mappings-of class-mapping-definition))))
    (when (not (null value-mapping-definition))
      (get-column-definition column-name value-mapping-definition))))

(defun find-many-to-one-column-definition (column-name
					   class-mapping-definition
					   class-mapping-definitions)
  (let ((many-to-one-definition
	 (find-if #'(lambda (many-to-one-definition)
		      (find column-name
			    (columns-names-of many-to-one-definition)))
		  (many-to-one-mappings-of class-mapping-definition))))
    (when (not (null many-to-one-definition))
      (find-column-definition
       (position column-name
		 (columns-names-of many-to-one-definition))
       (mapped-class-of many-to-one-definition)
       class-mapping-definitions))))

(defun find-primary-key-column-definition (column-position
					   class-mapping-definition
					   class-mapping-definitions)
  (let ((column-definition
	 (loop for mapped-superclass
	    in (mapped-superclasses-of class-mapping-definition)
	    thereis (find-primary-key-column-definition column-position
							(find mapped-superclass
							      class-mapping-definitions
							      :key #'mapped-class)
							class-mapping-definitions))))
    

;; infinite recursion
;; проверка тождественности стобцов PK (по определению) по иерархии наследования
;; проверить отсутствие исопльзования стобцов наследуемого первичного ключа в отображении значений
;; сделать ссылку  на определения отображений в mapping-schema, выорплнтиь рефаткоринг

(defun find-column-definition (column-position mapped-class
			       class-mapping-definitions)
  (let* ((class-mapping-definition
	  (find mapped-class class-mapping-definitions
		:key #'mapped-class-of))
	 (column-name
	  (elt (primary-key-of class-mapping-definition)
	       column-position)))
    (or (find-value-column-definition column-name
				      class-mapping-definition)
	(find-many-to-one-column-definition column-name
					    class-mapping-definition
					    class-mapping-definitions))))

(defun ensure-reference-column (name class-mapping-definition
				mapping-schema)
  (let ((table (get-table (table-name-of class-mapping-definition)
			  mapping-schema)))
    (multiple-value-bind (column presentp)
	(gethash name (columns-of table))
      (if (not presentp)
	  (add-column (or (find-column-definition
			   (position name (primary-key-of class-mapping-definition))
			   (mapped-class-of class-mapping-definition)
			   (class-mapping-definitions-of mapping-schema))
			  (error "Definition of column '~a' of table '~a' not found"
				 name (name-of table)))
		      table)
	  column))))

(defgeneric compute-reference-foreign-key (reference-definition mapping-schema))

(defmethod compute-reference-foreign-key ((many-to-one-definition many-to-one-mapping-definition)
					  mapping-schema)
  (let ((class-mapping-definition
	 (class-mapping-definition-of many-to-one-definition)))
    (make-instance 'foreign-key
		   :table (get-table
			   (table-name-of class-mapping-definition)
			   mapping-schema)
		   :columns (mapcar #'(lambda (name)
					(ensure-reference-column name
								 class-mapping-definition
								 mapping-schema))
				    (columns-names-of many-to-one-definition))
		   :referenced-table (table-of
				      (get-mapping
				       (mapped-class-of many-to-one-definition)
				       mapping-schema)))))

(defmethod compute-reference-foreign-key ((one-to-many-definition one-to-many-mapping-definition)
					  mapping-schema)
  (let ((mapped-class (mapped-class-of one-to-many-definition)))
    (make-instance 'foreign-key
		   :table (table-of
			   (get-mapping mapped-class mapping-schema))
		   :columns (mapcar #'(lambda (name)
					(ensure-reference-column name
								 (find-class-mapping-definition mapped-class)
								 mapping-schema))
				    (columns-names-of one-to-many-definition))
		   :referenced-table (get-table
				      (table-name-of
				       (class-mapping-definition-of one-to-many-definition))
				      mapping-schema))))

(defgeneric compute-association (many-to-one-definition
				 one-to-many-definition
				 mapping-schema))

(defmethod compute-association ((many-to-one-definition many-to-one-mapping-definition)
				(one-to-many-definition (eql nil))
				mapping-schema)
  (make-instance 'unidirectional-many-to-one-association
		 :foreign-key (compute-reference-foreign-key many-to-one-definition
							     mapping-schema)
		 :many-to-one-definition many-to-one-definition
		 :mapping-schema mapping-schema))

(defmethod compute-association ((many-to-one-definition (eql nil))
				(one-to-many-definition one-to-many-mapping-definition)
				mapping-schema)
  (make-instance 'unidirectional-one-to-many-association
		 :foreign-key (compute-reference-foreign-key one-to-many-definition
							     mapping-schema)
		 :one-to-many-definition one-to-many-definition
		 :mapping-schema mapping-schema))

(defmethod compute-association ((many-to-one-definition many-to-one-mapping-definition)
				(one-to-many-definition one-to-many-mapping-definition)
				mapping-schema)
  (make-instance 'bidirectional-association
		 :foreign-key (compute-reference-foreign-key many-to-one-definition
							     mapping-schema)
		 :many-to-one-definition many-to-one-definition
		 :one-to-many-definition one-to-many-definition
		 :mapping-schema mapping-schema))

(defmethod initialize-instance :after ((instance unidirectional-many-to-one-association)
				       &key many-to-one-definition
				       mapping-schema)
  (let ((many-to-one-mapping
	 (make-instance 'many-to-one-mapping
			:association instance
			:slot-name (slot-name-of many-to-one-definition)
			:unmarshaller (unmarshaller-of many-to-one-definition)
			:marshaller (marshaller-of many-to-one-definition)
			:referenced-class-mapping (get-mapping
						   (mapped-class-of many-to-one-definition)
						   mapping-schema))))
    (with-slots (many-to-one-direction) instance
      (setf many-to-one-direction many-to-one-mapping))
    (with-slots (reference-mappings)
	(get-mapping (mapped-class-of
		      (class-mapping-definition-of many-to-one-definition))
		     mapping-schema)
      (push many-to-one-mapping reference-mappings))))

(defmethod initialize-instance :after ((instance unidirectional-one-to-many-association)
				       &key one-to-many-definition
				       mapping-schema)
  (let ((one-to-many-mapping
	 (make-instance 'one-to-many-mapping
			:association instance
			:slot-name (slot-name-of one-to-many-definition)
			:unmarshaller (unmarshaller-of one-to-many-definition)
			:marshaller (marshaller-of one-to-many-definition)
			:referenced-class-mapping (get-mapping
						   (mapped-class-of one-to-many-definition)
						   mapping-schema))))
    (with-slots (one-to-many-direction) instance
      (setf one-to-many-direction one-to-many-mapping))
    (with-slots (reference-mappings)
	(get-mapping (mapped-class-of
		      (class-mapping-definition-of one-to-many-definition))
		     mapping-schema)
      (push one-to-many-mapping reference-mappings))))

(defun compute-reference-mappings (class-mapping-definitions mapping-schema)
  (loop for (many-to-one-definition . one-to-many-definition)
     in (compute-reference-pairs class-mapping-definitions)
     do (compute-association many-to-one-definition
			     one-to-many-definition
			     mapping-schema)))

;; 2. Добавить проверку первичного ключа по составу с PK таблиц наследуемых классов
;; 3. Добавить проверку отсутствия дублирования отображения колонки между отображением значения и отображением ссылки
;; колонки не должны фигурировать одновременно во внешних ключах и вотображении значений
;; 4. Проверить возможноть опредления первичного ключа таблицы, который бы включал колонки надкласса, а также дополнительные (в т.ч. наследование классов с разными PK)

;;(defun find-primary-key-column-definition (column-position
;;					   class-mapping-definition
;;					   class-mapping-definitions)
;;  (let ((column-definition
;;	 (loop for mapped-superclass
;;	    in (mapped-superclasses-of class-mapping-definition)
;;	    thereis (find-primary-key-column-definition column-position
;;							(find mapped-superclass
;;							      class-mapping-definitions
;;							      :key #'mapped-class)
;;							class-mapping-definitions))))
;;    (if (null column-definition)
;;	(

;;(defun ensure-primary-key-column (name class-mapping-definition
;;				  mapping-schema)
;;  (let ((table (get-table (table-name-of class-mapping-definition)
;;			  mapping-schema)))
;;    (multiple-value-bind (column presentp)
;;	(get-column name table)
;;      (if (not presentp)
;;	  (add-column (or (find-primary-key-column-definition
;;			   (position name (primary-key-of class-mapping-definition))
;;			   class-mapping-definition
;;			   (class-mapping-definitions-of mapping-schema))
;;			  (error "Definition of column '~a' of table '~a' not found"
;;				 name (name-of table)))
;;		      table)
;;	  column))))

(defun compute-inheritance-foreign-key (subclass-mapping superclass-mapping)
  (let ((table (table-of subclass-mapping)))
    (make-instance 'foreign-key
		   :table table
		   :columns (primary-key-of table)
		   :referenced-table (table-of superclass-mapping))))

(defun compute-inheritance-mapping (subclass-mapping superclass-mapping)
  (let ((inheritance-mapping
	 (make-instance 'inheritance-mapping
			:subclass-mapping subclass-mapping
			:superclass-mapping superclass-mapping
			:foreign-key (compute-inheritance-foreign-key subclass-mapping
								      superclass-mapping))))
    (with-slots (subclasses-mappings) superclass-mapping
      (push inheritance-mapping subclasses-mappings))
    inheritance-mapping))

;;    (with-slots (primary-key) (table-of subclass-mapping)
;;      (setf primary-key
;;	    (mapcar #'(lambda (column-name)
;;			(ensure-primary-key-column column-name
;;						 class-mapping-definition
;;						 mapping-schema))
;;		    (primary-key-of class-mapping-definition))))

(defun compute-inheritance-mappings (class-mapping-definition mapping-schema)
  (let ((subclass-mapping (get-mapping
			   (mapped-class-of class-mapping-definition)
			   mapping-schema)))
    (with-slots (superclasses-mappings) subclass-mapping
      (setf superclasses-mappings
	    (mapcar #'(lambda (superclass)
			(compute-inheritance-mapping subclass-mapping
						     (get-mapping superclass
								  mapping-schema)))
		    (mapped-superclasses-of class-mapping-definition))))))

(defmethod initialize-instance :after ((instance mapping-schema)
				       &key class-mapping-definitions)
  (let ((*mapping-schema* instance))
    (with-slots (tables class-mappings) instance
      (setf tables (compute-tables class-mapping-definitions)
	    class-mappings (compute-class-mappings class-mapping-definitions
						   instance)))
    (dolist (class-mapping-definition class-mapping-definitions)
      (compute-value-mappings class-mapping-definition instance))
    (compute-reference-mappings class-mapping-definitions instance)
    (dolist (class-mapping-definition class-mapping-definitions)
      (compute-inheritance-mappings class-mapping-definition instance))))

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