;;; cl-db.lisp

(in-package #:cl-db)

;; использовать классы *-mapping для строгого конфигурирования и нестрогого
;; или сделать конфигурирование в функциональном стиле

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (mapped-superclasses :initarg :mapped-superclasses
			:reader mapped-superclasses-of)
   (slot-mapping-definitions :initarg :slot-mapping-definitions
			     :reader slot-mapping-definitions-of)
   (value-mappings :initarg :value-mapping
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)))

(defun value-mapping-p (mapping)
  (typep mapping (find-class 'value-mapping-definition)))

(defun many-to-one-mapping-p (mapping)
  (typep mapping (find-class 'many-to-one-mapping-definition)))

(defun one-to-many-mapping-p (mapping)
  (typep mapping (find-class 'one-to-many-mapping-definition)))

(defun map-class (class-name table-name primary-key &key superclasses slots)
  (let ((mappings (mapcar #'mapping-of slots)))
    (make-instance 'class-mapping-definition
		   :table-name table-name
		   :primary-key primary-key
		   :mapped-class (find-class class-name)
		   :mapped-superclasses (mapcar #'find-class superclasses)
		   :slot-mapping-definitions slots
		   :value-mapping (remove-if-not #'value-mapping-p
						 mappings)
		   :many-to-one-mappings (remove-if-not #'many-to-one-mapping-p
							mappings)
		   :one-to-many-mappings (remove-if-not #'one-to-many-mapping-p
							mappings))))

(defclass slot-mapping-definition ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun map-slot (slot-name mapping &optional constructor reader)
  (make-instance 'slot-mapping-definition
		 :constructor constructor
		 :slot-name slot-name
		 :mapping mapping
		 :reader reader))

(defclass value-mapping-definition ()
  ((columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun value (column &rest columns)
  (make-instance 'value-mapping-definition :columns-names columns))

(defclass one-to-many-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun one-to-many (class-name column &rest columns)
  (make-instance 'one-to-many-mapping-definition
		 :mapped-class (find-class class-name)
		 :columns-names columns))

(defclass many-to-one-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun many-to-one (class-name column &rest columns)
  (make-instance 'many-to-one-mapping-definition 
		 :mapped-class (find-class class-name)
		 :columns-names columns))

(defclass class-mapping ()
  ((table :initarg :table
	  :reader table-of)
   (mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (superclasses-mappings :initarg :superclasses-mappings
			  :reader superclasses-mappings-of)
   (subclasses-mappings :initform (list)
			:reader subclasses-mappings-of)
   (slot-mappings :initarg :slot-mappings
		  :reader slot-mappings-of)
   (reference-mappings :initform (make-hash-table)
		       :reader reference-mappings-of)))

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initform (make-hash-table) :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initform (make-hash-table) :reader foreign-keys-of)))

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

(defclass value-column (column) ())

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun get-mapping-definition (mapped-class class-mapping-definitions)
  (gethash mapped-class class-mapping-definitions))

(defun get-mapping (mapped-class class-mappings)
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class class-mappings)
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defun compute-slot-mappings (class-mapping slot-mapping-definitions
			      class-mappings)
  (mapcar #'(lambda (definition)
	      (make-instance 'slot-mapping
			     :slot-name (slot-name-of definition)
			     :mapping (compute-slot-mapping (mapping-of definition)
							    class-mapping
							    class-mappings)))
	  slot-mapping-definitions))

(defun compute-mappings (class-mapping-definitions)
  (let ((class-mappings (reduce #'(lambda (hash-table definition)
				    (let ((mapped-class (mapped-class-of definition)))
				      (setf (gethash mapped-class hash-table)
					    (make-instance 'class-mapping
							   :mapped-class mapped-class
							   :table (make-instance 'table
										 :name (table-name-of definition)
										 :primary-key (primary-key-of definition)))))
				    hash-table)
				class-mapping-definitions
				:initial-value (make-hash-table :size (length class-mapping-definitions)))))
    (dolist (definition class-mapping-definitions class-mappings)
      (let ((class-mapping (get-mapping (mapped-class-of definition)
					class-mappings)))
	(with-slots (superclasses-mappings slot-mappings) class-mapping
	  (setf superclasses-mappings
		(mapcar #'(lambda (superclass)
			    (let ((superclass-mapping (get-mapping (mapped-class-of definition)
								   class-mappings)))
			      (push (subclasses-of superclass-mapping) class-mapping)
			      superclass-mapping))
			(mapped-superclasses-of definition))
		slot-mappings
		(compute-slot-mappings class-mapping
				       (slot-mapping-definitions-of definition)
				       class-mappings)))))))

(defgeneric compute-slot-mapping (mapping-definition
				  class-mapping
				  class-mapping-definitions))

(defclass value-mapping ()
  ((columns :initarg :columns :reader columns-of)))

(defun column-exist-p (table column-name)
  (gethash column-name (columns-of table)))

(defun add-column (table column)
  (setf (gethash (name-of column) (columns-of table)) column))

(defmethod make-value-column (table name)
  (if (not (column-exist-p table name))
      (add-column table (make-instance 'value-column :name name))
      (error "Column ~a already mapped" name)))

(defmethod compute-slot-mapping ((mapping-definition value-mapping-definition)
				 class-mapping class-mappings)
  (declare (ignore class-mappings))
  (let ((table (table-of class-mapping)))
    (make-instance 'value-mapping
		   :columns (mapcar #'(lambda (column-name)
					(make-value-column table
							   column-name))
				    (columns-names-of mapping-definition)))))

(defclass reference-mapping ()
  ((referenced-class-mapping :initarg :referenced-class-mapping
			     :reader referenced-class-mapping-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass one-to-many-mapping (reference-mapping) ())

(defun ensure-foreign-key-column (table column-name)
  (gethash column-name (columns-of table)
	   (setf (gethash column-name (columns-of table))
		 (make-instance 'foreign-key-column
				:table table :name column-name))))

(defun ensure-foreign-key (table columns-names referenced-table)
  (let* ((columns (mapcar #'(lambda (column-name)
			      (ensure-foreign-key-column table column-name))
			  columns-names))
	 (key (cons table columns))
	 (foreign-keys (foreign-keys-of table)))
    (gethash key foreign-keys
	     (setf (gethash key foreign-keys)
		   (make-instance 'foreign-key
				  :table table :columns columns
				  :referenced-table referenced-table)))))

(defmethod compute-slot-mapping ((mapping-definition one-to-many-mapping-definition)
				 class-mapping class-mappings)
  (declare (ignore class-mapping))
  (let ((referenced-class-mapping
	 (get-mapping (mapped-class-of mapping-definition)
		      class-mappings)))
    (make-instance 'one-to-many-mapping
		   :referenced-class-mapping referenced-class-mapping
		   :foreign-key (ensure-foreign-key (table-of referenced-class-mapping)
						    (columns-names-of mapping-definition)
						    (table-of class-mapping)))))

(defclass many-to-one-mapping (reference-mapping) ())

(defmethod compute-slot-mapping ((mapping-definition many-to-one-mapping-definition)
				 class-mapping class-mapping-definitions)
  (let ((referenced-class-mapping (get-mapping (mapped-class-of mapping-definition)
					       class-mapping-definitions)))
    (make-instance 'many-to-one-mapping
		   :referenced-class-mapping referenced-class-mapping
		   :foreign-key (ensure-foreign-key (table-of class-mapping)
						    (columns-names-of mapping-definition)
						    (table-of referenced-class-mapping)))))

;; наследование
;; в т.ч. проверка одинаковости первичного ключа и родительских классов

(defclass clos-session ()
  ((mappings :initarg :mappings :reader mappings-of)
   (connection :initarg :connection :reader connection-of)
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defun make-clos-session (connector &rest class-mappings)
  (make-instance 'clos-session
		 :connection (funcall connector)
		 :mappings (compute-mappings class-mappings)))

(defun get-class-mapping (session class)
  (multiple-value-bind (mapping present-p)
      (gethash class (mappings-of session))
    (if (not present-p)
	(error "Class ~a not mapped" class)
	mapping)))

(defclass table-reference ()
  ((table :initarg :table
	  :reader table-of)
   (alias :initarg :alias
	  :reader alias-of)))

(defvar *object-loader*)

(defun compute-superclass-loaders ())

(defun xyz (class-mapping)
  (make-instance 'object-loader
		 :superclasses-loaders (mapcar #'xyz (superclasses-mappings-of class-mapping))
		 :subclasses-loaders (mapcar #'xyz (superclasses-mappings-of class-mapping))
(defclass object-loader ()
  ((class-mapping :initarg :class-mapping
		  :reader :class-mappings)
   (table-reference :reader table-reference-of)
   (superclass-loaders :reader superclass-loaders-of)
   (subclass-loaders :reader superclass-loaders-of)))



(defmethod initialize-instance ((instance object-loader) &key class-mapping
				(superclasses-loaders (compute-superclasses-loaders class-mapping))
				(subclasses-loaders (compute-subclasses-loaders class-mapping)))
  (apply #'call-next-method instance
	 :class-mapping class-mapping
	 :subclasses-loaders subclasses-loaders
	 :superclasses-loaders superclasses-loaders))
  
  (with-slots (table-refrence superclass-loaders subclass-loaders)
      

(defun compute-object-loader (table-reference class-mapping)
  



(defvar *session*)

(defun call-with-session (session function)
  (let ((*session* session))
    (funcall function)))

(defmacro with-session ((session) &body body)
  `(call-with-session ,session #'(lambda () ,@body)))

;;(defun db-find-all (session class &rest fetch) 
;;  (let* ((class-mapping (get-class-mapping session class))
;;	 (result (execute (make-select-query class-mapping fetch)
;;			  (connection-of session))))
;;    (map 'list #'(lambda (table-row)
;;		   (load session class-mapping fetch table-row))
;;	 table-rows)))

;;(defun db-read (&key all)
;;  (db-find-all *session* (find-class all)))

(defclass table-expression ()
  ((table-reference :initarg :table-reference
		    :reader table-reference-of)
   (joins :initarg :joins
	  :reader joins-of)))

(defclass join (table-expression)
  ((column-pairs :initarg :column-pairs
		 :reader column-pairs-of)))

(defclass inner-join (join) ())

(defclass left-outer-join (join) ())

(defclass select-query ()
  ((table-expression :initarg :table-expression
		     :reader table-expression-of)
   (select-list-items :initarg :select-list-items
		      :reader select-list-items-of)))

(defun compute-column-pairs (table foreign-key)
  (mapcar #'cons
	  (primary-key-of table)
	  (alexandria:hash-table-keys (columns-of foreign-key))))

(defun compute-superclasses-joins (table-reference superclass-mappings)
  (let ((table (table-of table-reference)))
    (mapcar #'(lambda (superclass-mapping)
		(let* ((superclass-table (table-of superclass-mapping))
		       (superclass-table-reference (make-instance 'table-reference
								  :table superclass-table)))
		  (cons (make-instance 'inner-join
				       :table-reference table-reference
				       :joined-table-reference superclass-table-reference
				       :column-pairs (compute-columns-pairs superclass-table
									    (get-foreign-key table
											     (primary-key-of table)
											     superclass-table)))
			(compute-joins superclass-table-reference superclass-mapping))))
	    superclass-mappings)))

(defun compute-subclasses-joins (table-reference subclasses-mappings)
  (let ((table (table-of table-reference)))
    (mapcar #'(lambda (subclass-mapping)
		(let* ((subclass-table (table-of subclass-mapping))
		       (subclass-table-reference (make-instance 'table-reference
								:table subclass-table)))
		  (cons (make-instance 'left-outer-join
				       :table-reference table-reference
				       :joined-table-reference subclass-table-reference
				       :column-pairs (compute-columns-pairs subclass-table
									    (get-foreign-key subclass-table
											     (primary-key-of subclass-table)
											     table)))
			(compute-class-joins superclass-mapping))))
	    subclasses-mappings)))

(defun compute-fetch-joins (class-mapping joined-fetch)
  (mapcar #'(lambda (joined-fetch)
	      (make-instance 'left-outer-join
			     :foreign-key (foreign-key-of
					   (get-reference-mapping class-mapping
								  joined-fetch))))
	  joined-fetch))

(defclass joined-fetch ()
  ((reference-reader :initarg :reference-reader
		     :reader reference-reder-of)
   (joined-fetch :initarg :joined-fetch
		 :reader joine-fetch-of)))

(defun compute-joins (table-reference class-mapping)
  (append
   (compute-subclasses-joins table-reference
			     (superclasses-mappings-of class-mapping))
   (compute-superclasses-joins table-reference
			       (subclasses-mappings-of class-mapping))))

(defun make-select-query (class-mapping)
  (let* ((table-reference (make-instance 'table-reference
					 :table (table-of class-mapping)))
	 (table-expression (make-instance 'table-expression
					  :table-reference table-reference
					  :joins (compute-joins table-reference
								class-mapping))))
    (make-instance 'select-query
		   :select-list-items 
		   :table-expression table-expression)))

(defgeneric make-query-string (query))

;;(defmethod make-query-string ((query select-query))
;;  (format nil "SELECT ~{~a~^, ~} FROM ~a 

;; execute

;;defclass result - hash-table of table-rows on foreign0keys
;; primary-key inheritance
;;(defclass db-query ()
;;  ((table :initarg :table :reader table)
;;   (foreign-keys :initarg :foreign-keys :reader foreign-keys)))

;;(defclass table-row ()
;;  ((values :initarg :values
;;	   :reader values-of)
;;   (foreign-key-tables-rows :initarg :foreign-key-tables-rows
;;			    :reader foreign-key-tables-rows-of)))

;; load

;; наследование и инициализация слотов
;; объект какого класса создавать? При условии, что нет абстрактных классов
;; объекты по иерархии загружаются снизу-вверх из исключенных записей нижнего уровня


;; скорее всего функция будет рекурсивной (особенно если fetch будет с Join'ами)
;;(defun load (session class-mapping fetch result)
;;  (let ((object (allocate-instance (mapped-class-of class-mapping))))
;;    (cache object session)
;;    (maphash #'(lambda (slot-name slot-mapping)
;;		 (