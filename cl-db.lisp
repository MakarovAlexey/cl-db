;;;; cl-db.lisp

(in-package #:cl-db)

;; использовать классы *-mapping для строгого конфигурирования и нестрогого
;; или сделать конфигурирование в функциональном стиле

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (value-mappings :initarg :value-mapping
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (table-name :initarg :table-name
	       :reader table-name-of)))

(defun value-mapping-p (mapping)
  (typep mapping (find-class 'value-mapping-definition)))

(defun many-to-one-mapping-p (mapping)
  (typep mapping (find-class 'many-to-one-mapping-definition)))

(defun one-to-many-mapping-p (mapping)
  (typep mapping (find-class 'one-to-many-mapping-definition)))

(defun map-class (class-name table-name primary-key &rest slots)
  (let ((mappings (mapcar #'mapping-of slots)))
    (make-instance 'class-mapping-definition
		   :table-name table-name
		   :primary-key primary-key
		   :mapped-class (find-class class-name)
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
   (subclasses-mappings :initarg :subclasses-mappings
			:reader subclasses-mappings-of)
   (slot-mappings :initarg :slot-mappings
		  :reader slot-mappings-of)
   (reference-mappings :initform (make-hash-table)
		       :reader reference-mappings-of)))

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initarg :columns :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initform (list) :accessor foreign-keys-of)))

(defclass column ()
  ((name :initarg :name :reader name-of)
   (type-name :initarg :type-name :reader type-name-of)))

(defclass foreign-key-column (column)
  ((foreign-keys :initarg :foreign-keys :reader foreign-keys-of)))

(defclass foreign-key ()
  ((table :initarg :table
	  :reader table-of)
   (referenced-table :initarg :referenced-table
		     :reader referenced-table-of)))

(defclass value-column (column) ())

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun compute-mappings (class-mapping-definitions)
  (let ((class-mappings (reduce #'(lambda (hash-table definition)
				    (let ((mapped-class (mapped-class-of definition)))
				      (setf (gethash mapped-class hash-table)
					    (make-instance 'class-mapping
							   :mapped-class mapped-class
							   :table (make-instance 'table
										 :name (table-name-of definition)
										 :primary-key (primary-key-of class-mapping-definition)))))
				    hash-table)
				class-mapping-definitions
				:initial-value (make-hash-table :size (length class-mapping-definitions)))))
    (dolist (definition class-mapping-definitions class-mappings)
      (with-slots (superclasses-mappings slots-mappings)
	  (gethash (mapped-class-of definition) class-mappings)
	(setf slots-mappings
	      (mapcar #'(lambda (definition)
			  (make-instance 'slot-mapping
					 :slot-name (slot-name-of definition)
					 :mapping (compute-mapping (mapping-of definition)
								   definition
								   class-mappings)))
		      (slot-mappings-of definition)))))))

(defgeneric compute-slot-mapping (mapping-definition
				  class-mapping
				  class-mapping-definitions))

(defclass value-mapping ()
  ((columns :initarg :columns :reader columns-of)))

(defun column-exist-p (table column-name)
  (gethash name (columns-of table)))

(defun add-column (table column)
  (setf (gethash name (columns-of table)) column))

(defmethod make-value-column (table name)
  (if (not (column-exist-p table name))
      (add-column table (make-instance 'value-column
				       :table table :name name))
      (error "Column ~a already mapped" name)))

(defmethod compute-slot-mapping ((mapping-definition value-mapping-definition)
				 class-mapping class-mapping-definitions)
  (declare (ignore class-mapping-definitions))
  (make-instance 'value-mapping
		 :class-mapping class-mapping
		 :columns (mapcar #'(lambda (column-name)
				      (make-value-column instance column-name
							 (table-of class-mapping)))
				  (columns-names-of mapping-definition))))

(defclass reference-mapping ()
  ((referenced-class-mapping :initarg :referenced-class-mapping
			     :reader referenced-class-mapping-of)))

(defclass one-to-many-mapping ()
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defun get-mapping (mapped-class class-mapping-definitions)
  (gethash mapped-class class-mapping-definitions))

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
				 class-mapping class-mappings-definitions)
  (declare (ignore class-mapping))
  (let ((referenced-class-mapping (get-mapping (mapped-class-of mapping-definition)
					       class-mapping-definitions)))
    (make-instance 'one-to-many-mapping
		   :referenced-class-mapping referenced-class-mapping
		   :foreign-key (ensure-foreign-key (table-of referenced-class-mapping)
						    (columns-names-of mapping-definition)
						    (table-of class-mapping)))))

(defclass many-to-one-mapping ()
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

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
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defun make-clos-session (connector &rest class-mappings)
  (make-instance 'clos-session
		 :connection (funcall connector)
		 :mappings (reduce #'(lambda (table class-mapping)
				       (setf (gethash (mapped-class-of class-mapping) table)
					     class-mapping)
				       table)
				   class-mappings
				   :initial-value (make-hash-table :size (length class-mappings)))))

(defvar *session*)

(defun db-read (&key all)
  (db-find-all *session* (find-class all)))

(defun get-class-mapping (session class)
  (gethash class (mappings-of session)))

(defclass join ()
  ((foreign-key :initarg :foreign-key :reader foreign-key-of)))

(defclass inner-join (join) ())

(defun compute-superclasses-joins (class-mapping joined-fetch)
  (let ((table (table-of class-mapping)))
    (mapcar #'(lambda (superclass-mapping)
		(cons (make-instance 'inner-join
				     :foreign-key (get-foreign-key table
								   (primary-key-of table)
								   (table-of superclass-mapping)))
		      (compute-class-joins superclass-mapping joined-fetch)))
	    (superclasses-mappings-of class-mapping))))

(defclass left-outer-join (join) ())

(defun compute-subclasses-joins (class-mapping)
  (let ((table (table-of class-mapping)))
    (mapcar #'(lambda (subclass-mapping)
		(let ((subclass-table (table-of subclass-mapping)))
		  (cons (make-instance 'left-outer-join
				       :foreign-key (get-foreign-key subclass-table
								     (primary-key-of subclass-table)
								     table))
			(compute-class-joins superclass-mapping))))
	    (superclasses-mappings-of class-mapping))))

(defun compute-fetch-joins (class-mapping joined-fetch)
  (;; 

(defclass joined-fetch ()
  ((slot-mapping :initarg :slot-mapping :reader slot-mapping-of)
   (fetchings :initarg :fetchings :reader fetchings-of)))

(defun compute-class-joins (class-mapping joined-fetch)
  (flet ((mapping-reference-p (joined-fetch)
	   (reference-mapping-p (reference-reader joined-fetch)
				class-mapping)))
    (append (compute-superclasses-joins class-mapping
					(remove-if #'mapping-reference-p
						   joined-fetch))
	    (compute-fetch-joins class-mapping
				 (remove-if-not #'reference-mapping-p
						joined-fetch))
	    (compute-subclasses-joins class-mapping))))

(defun build-select (class-mapping joined-fetch)
  (make-instance 'select-query :table (table-of class-mapping)
		 :joins (compute-class-joins class-mapping fetch)))

(defun query-select (class-mapping fetch connection)
  (execute (build-select class-mapping fetch) connection))

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

;;(defun db-find-all (session class &rest fetch) 
;;  (let* ((class-mapping (get-class-mapping session class))
;;	 (result (query-select class-mapping fetch
;;			       (connection-of session))))
;;    (map 'list #'(lambda (table-row)
;;		   (load session class-mapping fetch table-row))
;;	 table-rows)))

;; execute
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