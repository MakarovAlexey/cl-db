;;;; cl-db.lisp

(in-package #:cl-db)

;; использовать классы *-mapping для строгого конфигурирования и нестрогого
;; или сделать конфигурирование в функциональном стиле

;;(defclass persistence-unit ()
;;  ((direct-mappings :initarg :mappings :reader effective-mappings-of)
;;   (tables :initarg :tables :reader tables-of)
;;   (effective-mappings :initarg :mappings :reader effective-mappings-of)))

(defclass direct-class-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (superclasses :initarg :superclasses :reader superclasses-of)
   (slot-mappings :initarg :slot-mappings :reader slot-mappings-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (table-name :initarg :table-name :reader table-name-of)))

(defun map-class (class-name &key table-name primary-key superclasses slots)
  (make-instance 'class-mapping
		 :class-name class-name
		 :superclasses superclasses
		 :slot-mappings slots
		 :table-name table-name
		 :primary-key primary-key))

(defclass direct-slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (direct-mapping :initarg :mapping :reader direct-mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun map-slot (slot-name mapping &optional constructor reader)
  (make-instance 'slot-mapping :slot-name slot-name
		 :mapping mapping
		 :constructor serializer
		 :reader deserializer))

(defclass direct-value-mapping ()
  ((columns-names :initarg :columns-names :reader columns-names-of)))

(defun value (&rest columns)
  (make-instance 'value-mapping :columns columns))

(defclass direct-one-to-many-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns-names :initarg :columns-names :reader columns-names-of)))

(defun one-to-many (class-name &rest columns)
  (make-instance 'one-to-many :class-name class-name :columns columns))

(defclass direct-many-to-one-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns-names :initarg :columns-names :reader columns-names-of)))

(defun many-to-one (class-name &rest columns)
  (make-instance 'many-to-one :class-name class-name :columns columns))

(defclass persistent-unit ()
  ((direct-class-mappings :initarg :direct-class-mappings
			  :reader direct-class-mappings-of)))

(defun reduce-to-hash-table (hash-function sequence)
  (reduce #'(lambda (hash-table object)
	      (setf (gethash (funcall hash-function object) hash-table)
		    object)
	      hash-table)
	  sequence
	  :initial-value (make-hash-table :size (length sequence))))

(defun make-persistence-unit (&rest direct-class-mappings)
  (let* ((tables
	  (reduce-to-hash-table #'table-name-of
				(map 'list
				     #'(lambda (direct-class-mapping)
					 (compute-table direct-class-mapping
							direct-class-mappings))
				     direct-class-mappings)))
	 (effective-class-mappings
	  (reduce-to-hash-table #'class-name-of
				(map 'list
				     #'(lambda (direct-class-mapping)
					 (compute-effective-class-mapping direct-class-mapping
									  (gethash (table-name-of direct-class-mapping) tables)))
				     direct-class-mappings))))
    (maphash #'(lambda (class-name effective-class-mapping)
		 (declare (ignore class-name))
		 (with-slots (superclasses-mappings many-to-one-mappings one-to-many-mappings) effective-class-mapping
		   (setf superclasses-mappings
			 (compute-superclasses-mappings effective-class-mapping
							effective-class-mappings)
			 many-to-one-mappings
			 (compute-many-to-one-mappings effective-class-mapping
						       effective-class-mappings)
			 many-to-one-mappings
			 (compute-one-to-many-mappings effective-class-mapping
						       effective-class-mappings))))
	     effective-class-mappings)
    (make-instance 'persistence-unit :tables tables
		   :direct-class-mappings direct-class-mappings
		   :effective-class-mappings effective-class-mappings)))

(defun make-effective-class-mapping (					 (make-instance 'effective-class-mapping
							:class-name (class-name-of direct-class-mapping)
							:table (gethash (table-name-of direct-class-mapping) tables)))))))

(defclass effective-class-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (superclasses-names :initarg :superclasses :reader superclasses-of)
   (value-mappings :initarg :value-mappings :reader value-mappings-of)
   (reference-mappings :initarg :reference-mappings :reader reference-mappings-of)
   (collection-mappings :initarg :collection-mappings :reader collection-mappings-of)
   (table :initarg :table :reader table-of)))

(defun compute-effective-class-mapping (direct-class-mapping table direct-class-mappings)
  
  (reduce #'(lambda (tables class-direct-mapping)
	      (let ((table-name (table-name-of class-direct-mapping))
		    (foreign-keys (compute-foreign-keys (class-name-of class-direct-mapping)
							class-direct-mappings)))
		(setf (gethash table-name tables)
		      (make-instance 'table :name table-name
				     :foregn-keys foreign-keys
				     :columns (append (reduce #'append
							      (mapcar #'columns-of
								      (value-mappings-of class-direct-mapping)))
						      (reduce #'append
							      (mapcar #'columns-of foreign-keys)))))))))

(defclass column ()
  ((name :initarg :name :reader name-of)))

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initarg :columns :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initarg :foreign-keys :reader foreign-keys-of)))

;; загрузка начинается с отображения не имеющего ассоциаций many-to-one



;;доинициализировать коллекции при создании зависимых объектов

(defclass effective-slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (effective-mapping :initarg :mapping :reader effective-mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defclass effective-value-mapping ()
  ((columns :initarg :columns :reader columns-of)))

(defclass effective-one-to-many-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns :initarg :columns :reader columns-of)))

(defclass effective-many-to-one-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns :initarg :columns :reader columns-of)))

;;(defun make-persistence-unit (&rest class-mappings)
;;  (reduce #'compute-mapping class-mappings
;;	  :initial-value (make-instance 'persistence-unit)))

;(defun compute-mapping (persistence-unit class-mapping)
;  (make-instance 'persistence-unit
;		 
	;	 :tables (compute-tables (tables-of persistence-unit))
	;	 :class-mappings
		 ;;(lambda (persistence-unit class-mapping)
	      ;(make-instance 'persistence-unit :tables



;;compute-columns table-name mappings)))))))

;;(defun compute-columns (table-name class-mappings)
;;  (reduce #'(lambda (columns slot-mapping-definition)
	      

(defun column (name) ;;? может для определения типа столбца
  (make-instance 'column :name name))

(defclass clos-session ()
  ((mappings :initarg :mappings :reader mappings-of)
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defun make-clos-session (connector &rest class-mappings)
  (make-instance 'clos-session
		 :connection (funcall connector)
		 :cache (make-hash-table :size (length class-mappings))
		 :mappings (reduce #'(lambda (table class-mapping)
				       (setf (gethash (class-name-of class-mapping) table)
					     class-mapping)
				       table)
				   class-mappings
				   :initial-value (make-hash-table :size (length class-mappings)))))

(defvar *session*)

(defun db-read (&key all)
  (db-find-all all))

(defun db-find-all (session class-name)
  (let ((class-mapping (gethash class-name (mappings-of session))))
    (map 'list #'(lambda (row)
		   (load (reduce #'(lambda (table class-mapping)
				       (setf (gethash (class-name-of class-mapping) table)
					     class-mapping)
				       table)
				   class-mappings
				   :initial-value (make-hash-table :size (length class-mappings)))))

(defvar *session*)

(defun db-read (&key all)
  (db-find-all all))

(defun db-find-all (session class-name)
  (let ((class-mapping (gethash class-name (mappings-of session))))
    (map 'list #'(lambda (row)
		   (load row class-mapping session))
	 (execute (connection-of session)
		  (make-select-query (table-of class-mapping))))))

(defun make-select-query (table)
  (let ((table-name (name-of table)))
    (format nil "SELECT ~{~a~^, ~} FROM ~a"
	    (map 'list #'(lambda (column)
			   (format nil "~a.~a" table-name (name-of column)))
		 (columns-of table)) table-name)))

(defclass clos-transaction ()
  ((clos-session :initarg :clos-session :reader clos-session-of)
   (new-objects :initform (list) :accessor new-objects-of)
   (objects-snapshots :initarg :objects-snapshots :reader object-snapshots)))

;;(defun begin-transaction (&optional (session *session*))
;;  (make-instance 'clos-transaction :clos-session clos-session
;;		 :objects-snapshots (make-snapshot (list-loaded-objects session)
;;						   (mappings-of session))))

;;(defun list-loaded-objects (session)
  

;;(defun persist (object &optional (transaction *transaction*))
;;  (when (not (loaded-p object (clos-session-of transaction)))
;;    (push (new-objects-of transaction) object)))



;;(defgeneric db-query (connection query))

;;(defun load (mapping row))

;;  (query-db (connection-of session)
;;	    (get-mapping class-name)

;;(defun db-get (class-name &rest primary-key)

;;

;;(with-session (*session*)
;;  (db-read :all 'project))