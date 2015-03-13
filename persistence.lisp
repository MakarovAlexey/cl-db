(in-package #:cl-db)

;; implement cascade operations
(defclass instance-state ()
  ((object :initarg :object
	   :reader object-of)
   (mapping :initarg :mapping
	    :reader mapping-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (properties :initarg :properties
	       :reader object-properties-of)
   (many-to-one :initarg :many-to-one
		:reader many-to-one-of)
   (one-to-many :initarg :one-to-many
		:reader one-to-many-of)
   (inverted-one-to-many :initform (list)
			 :accessor inverted-one-to-many-of
			 :documentation
			 "Many-to-one side of other one-to-many relations")))

(defclass new (instance-state)) ;; хранит состояние полностью

(defclass dirty (instance-state)) ;; хранит изменения в состоянии

(defclass removed (instance-state)) ;;

(defun slot-name-of (slot-mapping)
  (first slot-mapping))

(defun map-values (object slot-mappings)
  (mapcar #'(lambda (slot-mapping)
	      (list* (slot-value object (slot-name-of slot-mapping))
		     slot-mapping))
	  slot-mappings))

(defun map-object (object primary-key-value &rest mapping
		   &key properties one-to-many-mappings
		     many-to-one-mappings &allow-other-keys)
  (make-instance 'object-map
		 :object object
		 :mapping mapping
		 :primary-key primary-key-value
		 :properties (map-values object properties)
		 :many-to-one (map-values object many-to-one-mappings)
		 :one-to-many (map-values object one-to-many-mappings)))

(defun ensure-object-map (object-maps object mapping-schema
			  class-name &rest primary-key)
  (let ((key (list class-name object)))
    (multiple-value-bind (map presentp)
	(gethash key object-maps)
      (if (not presentp)
	  (setf (gethash key object-maps)
		(apply #'map-object object primary-key
		       (list* :class-name
			      (get-class-mapping class-name
						 mapping-schema))))
	  map))))

(defun referenced-class-of (slot-mapping)
  (getf (rest slot-mapping) :referenced-class-name))

(defun compute-inverted-one-to-many (objects-table object
				     referenced-object
				     &rest slot-mapping)
  (push (list* object slot-mapping)
	(inverted-one-to-many-of
	 (gethash (list (referenced-class-of slot-mapping)
			referenced-object)
		  objects-table))))

(defun get-object-of (key)
  (second key))

;; создавать вместо "карты" объекта ассоциативный список по столбцам
;; таблицы, при фиксации просто вычисляем разницу между списками
;; #'set-difference

;; проверка согласованности значений слотов
(defun map-objects (objects-table mapping-schema)
  (let ((object-maps
	 (make-hash-table :size (hash-table-size objects-table)
			  :test #'equal)))
    (maphash #'(lambda (pk-and-class-name object)
		 (apply #'ensure-object-map
			object-maps object mapping-schema
			pk-and-class-name))
	     objects-table)
    (maphash #'(lambda (key object-map)
		 (dolist (one-to-many (one-to-many-of object-map))
		   (apply #'compute-inverted-one-to-many
			  objects-table (get-object-of key) one-to-many)))
	     object-maps)
    object-maps))

(defun insert-object (object class-name
		      &key properties one-to-many-mappings
		      many-to-one-mappings superclass-mappings)
  (

(defun flush-session (session)
  (let ((dirty-state
	 (make-hash-table :size (hash-table-size
				 (count-objects session)))))
  (reduce #'(lambda (result object)
	      (apply #'insert-object object
		     (get-class-mapping
		      (class-name
		       (class-of object)) 
		      (mapping-schema-of sesion))))
	  (new-objects-of session)))
