;;;; cl-db.lisp

(in-package #:cl-db)

;; использовать классы *-mapping для строгого конфигурирования и нестрогого
;; или сделать конфигурирование в функциональном стиле

(defclass direct-class-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (direct-slot-mappings :initarg :slot-mappings :reader direct-slot-mappings-of)
   (value-mappings :reader value-mappings-of)
   (many-to-one-mappings :reader many-to-one-of)
   (one-to-many-mappings :reader one-to-many-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (table-name :initarg :table-name :reader table-name-of)))

(defun map-class (class-name &key table-name primary-key slots)
  (make-instance 'direct-class-mapping
		 :class-name class-name
		 :slot-mappings slots
		 :table-name table-name
		 :primary-key primary-key))

(defmethod shared-initialize :after ((instance direct-class-mapping) slot-names
				     &key slot-mappings)
  (with-slots (value-mappings many-to-one-mappings one-to-many-mappings)
      (let ((mappings (mapcar #'mapping-of slot-mappings)))
	(setf value-mappings
	      (remove-if-not #'(lambda (mapping)
				 (typep mapping 'value-mapping))
			     mappings)
	      many-to-one-mappings
	      (remove-if-not #'(lambda (mapping)
				 (typep mapping 'value-mapping))
			     mappings)
	      one-to-many-mappings
	      (remove-if-not #'(lambda (mapping)
				 (typep mapping 'value-mapping))
			     mappings)))))

(defclass direct-slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (direct-mapping :initarg :mapping :reader direct-mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun map-slot (slot-name mapping &optional constructor reader)
  (make-instance 'direct-slot-mapping :slot-name slot-name
		 :mapping mapping
		 :constructor constructor
		 :reader reader))

(defclass direct-value-mapping ()
  ((columns-names :initarg :columns-names :reader columns-names-of)))

(defun value (&rest columns)
  (make-instance 'direct-value-mapping :columns-names columns))

(defclass direct-one-to-many-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns-names :initarg :columns-names :reader columns-names-of)))

(defun one-to-many (class-name &rest columns)
  (make-instance 'direct-one-to-many-mapping
		 :class-name class-name :columns-names columns))

(defclass direct-many-to-one-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (columns-names :initarg :columns-names :reader columns-names-of)))

(defun many-to-one (class-name &rest columns)
  (make-instance 'direct-many-to-one-mapping
		 :class-name class-name :columns-names columns))

(defclass persistentce-unit ()
  ((direct-class-mappings :initarg :direct-class-mappings
			  :reader direct-class-mappings-of)))

(defclass effective-class-mapping ()
  ((table :initarg :table :reader table-of)))

(defclass persistence-unit ()
  ((direct-class-mappings :initarg :direct-class-mappings
		    :reader effective-mappings-of)
   (tables :initarg :tables
	   :reader tables-of)
   (effective-class-mappings :initarg :effective-class-mappings
		       :reader effective-mappings-of)))
	  
(defun make-persistence-unit (&rest direct-class-mappings)
  (make-instance 'persistence-unit
		 :direct-class-mappings direct-class-mappings))

(defmethod shared-initialize :after ((instance persistentce-unit) slot-names
				     &key direct-class-mappings)
  (with-slots (tables) instance ;;effective-class-mappings)
    (setf tables (reduce #'(lambda (alist direct-slot-mapping)
			     (let ((table-name (table-name-of direct-slot-mapping))
				   (columns (compute-columns table-name direct-class-mappings)))
			       (acons table-name
				      (make-instance 'table :name table-name :columns columns
						     :primary-key (map 'list
								       #'(lambda (column-name)
									   (assoc column-name columns))
								       (primary-key-of direct-slot-mapping)))
				      alist)))
			 direct-class-mappings))))

(defun compute-table-columns (direct-class-mapping direct-class-mappings)
  (append (compute-value-columns direct-class-mapping)
	  (compute-many-to-one-columns direct-class-mapping)
	  (compute
  (append (mapcar #'columns-of
		  (remove-if #'(lambda (mapping)
				 (typep mapping 'direct-one-to-many-mapping))
			     (mapcar #'direct-mapping-of
				     (direct-slot-mappings-of direct-class-mapping))))
	  (mapcar #'columns-of
		  (remove-if-not #'(lambda (mapping)
				     (typep mapping 'direct-one-to-many-mapping))
			     (mapcar #'direct-mapping-of
				     (direct-slot-mappings-of direct-class-mapping))))
		  
 ;;   :foregn-keys (compute-foreign-keys table-name columns direct-class-mappings))


(defun compute-table (direct-class-mapping direct-class-mappings)
  (make-instance 'table :name (table-name-of direct-class-mapping)
		 :columns (apply #'append
				 (mapcar #'columns-names-of
					 (direct-slot-mappings-of class-direct-mapping)))))
;;				  (apply #'append
;;					 (mapcar #'columns-names-of
;;						 (direct-class-mapping))
;;				  (apply #'append
;;					 (mapcar #'(lambda (direct-class-mapping)
;;						     (compute-foreign-key-columns-names-of direct-class-mappings)
						 
;;		 :columns (reduce #'(lambda (direct-class-mapping)
;;				      (
;;  (reduce #'(lambda (tables class-direct-mapping)
;;	      (let ((table-name (table-name-of class-direct-mapping))
;;		    (foreign-keys (compute-foreign-keys (class-name-of class-direct-mapping)
;;							class-direct-mappings)))
;;		(setf (gethash table-name tables)
;;		      (make-instance 'table :name table-name
;;				     :foregn-keys foreign-keys
;;				     

;;(reduce-to-hash-table #'table-name-of
;;		      (map 'list
;;			   #'(lambda (direct-class-mapping)
;;			       (compute-table ))
;;			   direct-class-mappings)))

;;   (class-name :initarg :class-name :reader class-name-of)
;;   (superclasses-names :initarg :superclasses :reader superclasses-of)
;;   (value-mappings :initarg :value-mappings :reader value-mappings-of)
;;   (reference-mappings :initarg :reference-mappings :reader reference-mappings-of)
;;   (collection-mappings :initarg :collection-mappings :reader collection-mappings-of)
;;   ))

;;	      effective-class-mappings (compute-effective-class-mappings direct-class-mappings
;;									 computed-tables)))))

;;(defun reduce-to-hash-table (key sequence)
;;  (reduce #'(lambda (hash-table object)
;;	      (setf (gethash (funcall key object) hash-table) object)
;;	      hash-table)
;;	  sequence
;;	  :initial-value (make-hash-table :size (length sequence))))

;;(defun compute-effective-class-mappings (direct-class-mappings tables)
;;  (let ((effective-class-mappings (reduce-to-hash-table #'class-name-of
;;							(map 'list
;;							     #'(lambda (direct-class-mapping)
;;								 (compute-effective-class-mapping direct-class-mapping
;;												  (gethash (table-name-of direct-class-mapping) tables)))
;;							     direct-class-mappings))))
;;    (maphash #'(lambda (class-name effective-class-mapping)
;;		 (declare (ignore class-name))
;;		 (with-slots (superclasses-mappings many-to-one-mappings one-to-many-mappings) effective-class-mapping
;;		   (setf superclasses-mappings
;;			 (compute-superclasses-mappings effective-class-mapping
;;							effective-class-mappings)
;;			 many-to-one-mappings
;;			 (compute-many-to-one-mappings effective-class-mapping
;;						       effective-class-mappings)
;;			 one-to-many-mappings
;;			 (compute-one-to-many-mappings effective-class-mapping
;;						       effective-class-mappings))))
;;	     effective-class-mappings)))

;;(defun make-effective-class-mapping (					 (make-instance 'effective-class-mapping
;;							:class-name (class-name-of direct-class-mapping)
;;							:table (gethash (table-name-of direct-class-mapping) tables)))))))



;;(defun compute-effective-class-mapping (direct-class-mapping table direct-class-mappings)
;;  (reduce #'(lambda (tables class-direct-mapping)
;;	      (let ((table-name (table-name-of class-direct-mapping))
;;		    (foreign-keys (compute-foreign-keys (class-name-of class-direct-mapping)
;;							class-direct-mappings)))
;;		(setf (gethash table-name tables)
;;		      (make-instance 'table :name table-name
;;				     :foregn-keys foreign-keys
;;				     :columns (append (reduce #'append
;;							      (mapcar #'columns-of
;;								      (value-mappings-of class-direct-mapping)))
;;						      (reduce #'append
;;							      (mapcar #'columns-of foreign-keys)))))))))
;;

;; загрузка начинается с отображения не имеющего ассоциаций many-to-one



;;доинициализировать коллекции при создании зависимых объектов

;;(defclass effective-slot-mapping ()
;;  ((slot-name :initarg :slot-name :reader slot-name-of)
;;   (effective-mapping :initarg :mapping :reader effective-mapping-of)
;;   (constructor :initarg :constructor :reader constructor-of)
;;   (reader :initarg :reader :reader reader-of)))

;;(defclass effective-value-mapping ()
;;  ((columns :initarg :columns :reader columns-of)))

;;(defclass effective-one-to-many-mapping ()
;;  ((class-name :initarg :class-name :reader class-name-of)
;;   (columns :initarg :columns :reader columns-of)))

;;(defclass effective-many-to-one-mapping ()
;;  ((class-name :initarg :class-name :reader class-name-of)
;;   (columns :initarg :columns :reader columns-of)))

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
	      

;;(defun column (name) ;;? может для определения типа столбца
;;  (make-instance 'column :name name))

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

;;(defun db-read (&key all)
;;  (db-find-all all))

;;(defun db-find-all (session class-name)
;;  (let ((class-mapping (gethash class-name (mappings-of session))))
;;    (map 'list #'(lambda (row)
;;		   (load (reduce #'(lambda (table class-mapping)
;;				       (setf (gethash (class-name-of class-mapping) table)
;;					     class-mapping)
;;				       table)
;;				   class-mappings
;;				   :initial-value (make-hash-table :size (length class-mappings))))))))

;;(defvar *session*)

;;(defun db-read (&key all)
;;  (db-find-all all))

;;(defun db-find-all (session class-name)
;;  (let ((class-mapping (gethash class-name (mappings-of session))))
;;    (map 'list #'(lambda (row)
;;		   (load row class-mapping session))
;;	 (execute (connection-of session)
;;		  (make-select-query (table-of class-mapping))))))

;;(defun make-select-query (table)
;;  (let ((table-name (name-of table)))
;;    (format nil "SELECT ~{~a~^, ~} FROM ~a"
;;	    (map 'list #'(lambda (column)
;;			   (format nil "~a.~a" table-name (name-of column)))
;;		 (columns-of table)) table-name)))

;;(defclass clos-transaction ()
;;  ((clos-session :initarg :clos-session :reader clos-session-of)
;;   (new-objects :initform (list) :accessor new-objects-of)
;;   (objects-snapshots :initarg :objects-snapshots :reader object-snapshots)))

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