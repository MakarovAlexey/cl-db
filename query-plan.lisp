(in-package #:cl-db)

;;; Query root

(defclass class-node ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (superclass-nodes :reader superclass-nodes-of)))

(defmethod initialize-instance :after ((instance class-node)
				       &key class-mapping
					 &allow-other-keys)
  (with-slots (superclass-nodes)
      instance
    (setf superclass-nodes
	  (mapcar #'(lambda (superclass-mapping)
		      (make-superclass-node superclass-mapping instance))
		  (superclass-mappings-of class-mapping)))))

(defclass superclass-node (class-node)
  ((class-node :initarg :class-node
	       :reader class-node-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defun make-superclass-mapping (superclass-mapping class-node)
  (let ((class-mapping (get-class-mapping
			(reference-class-of superclass-mapping))))
    (make-instance 'superclass-node
		   :foreign-key (foreign-key-of superclass-mapping)
		   :class-mapping class-mapping
		   :class-node class-node)))

(defclass concrete-class-node (class-node)
  ((reference-slots :reader reference-slots-of)
   (precedence-list  :initarg :precedence-list
		     :reader precedence-list-of)))

(defclass mapped-slot ()
  ((class-node :initarg :class-node
	       :reader class-node-of)))

(defclass property-slot (mapped-slot)
  ((property-mapping :initarg :property-mapping
		     :reader property-mapping-of)))

(defclass reference-slot (mapped-slot)
  ((reference-mapping :initarg :reference-mapping
		      :reader reference-mapping-of)))

(defun compute-precedence-list (precedence-list class-node)
  (let ((superclass-nodes
	 (set-difference
	  (superclass-nodes-of class-node) precedence-list)))
    (reduce #'compute-precedence-list superclass-nodes
	    :initial-value (append superclass-nodes
				   precedence-list))))

(defun compute-reference-slots (class-node)
  (let ((class-mapping
	 (class-mapping-of class-node)))
    (mapcar #'(lambda (reference-mapping)
		(make-instance 'reference-slot
			       :reference-mapping reference-mapping
			       :class-node class-node))
	    (append
	     (many-to-one-mappings-of class-mapping)
	     (one-to-many-mappings-of class-mapping)))))

(defmethod initialize-instance :after ((instance concrete-class-node)
				       &key superclass-precedence-list
					 &allow-other-keys)
  (with-slots (precedence-list reference-slots)
      instance
    (setf precedence-list
	  (compute-precedence-list
	   (list* instance superclass-precedence-list) instance))
    (setf reference-slots
	  (reduce #'append precedence-list
		  :key #'compute-reference-slots))))

(defclass root-node (concrete-class-node)
  ((property-slots :reader property-slots-of)))

(defun get-property-slot (root-node reader)
  (let* ((class-name
	  (class-name-of (class-mapping-of root-node)))
	 (slot-name
	  (get-slot-name (find-class class-name) reader)))
    (or
     (find slot-name
	   (property-slots-of root-node)
	   :key #'(lambda (property-node)
		    (slot-name-of
		     (mapping-of property-node))))
     (error "Property mapping for slot-name ~a of class mapping ~a not found"
	    slot-name class-name))))

(defun compute-property-slots (class-node)
  (mapcar #'(lambda (property-mapping)
	      (make-instance 'property-slot
			     :property-mapping property-mapping
			     :class-node class-node))
	  (property-mappings-of
	   (class-mapping-of class-node))))

(defmethod initialize-instance :after ((instance root-node)
				       &key &allow-other-keys)
  (with-slots (property-slots)
      instance
    (setf property-slots
	  (reduce #'append (precedence-list-of instance)
		  :key #'compute-property-slots))))

(defun make-root (class-name)
  (make-instance 'root-node
		 :class-mapping (get-class-mapping class-name)))

;;; Common

(defun find-slot-name (class reader-name)
  (or
   (find-if #'(lambda (slot-definition)
		(find reader-name
		      (slot-definition-readers slot-definition)))
	    (class-direct-slots class))
   (find-slot-name (find-if #'(lambda (class)
				(find-slot-name class reader-name))
			    (class-direct-superclasses class))
		   reader-name)))

(defun get-slot-name (class reader)
  (let ((slot-definition
	 (find-slot-name class (generic-function-name reader))))
    (when (null slot-definition)
      (error "Slot with reader ~a not found for class ~a"
	     reader (class-name class)))
    (slot-definition-name slot-definition)))

;;; Joins

(defclass reference-node (root-node)
  ((reference-slot :initarg :reference-slot
		   :reader reference-slot-of)))

(defun join (root-node reader &key alias join)
  (let* ((reference-slot
	  (find (get-slot-name
		 (find-class
		  (class-name-of
		   (class-mapping-of root-node))) reader)
		(reference-slots-of root-node)
		:key #'(lambda (reference-slot)
			 (slot-name-of
			  (reference-mapping-of reference-slot)))))
	 (reference-node
	  (make-instance 'reference-node
			 :class-mapping (get-class-mapping
					 (reference-class-of
					  (reference-mapping-of reference-slot)))
			 :reference-slot reference-slot))
	 (args
	  (when (not (null join))
	    (multiple-value-call #'append
	      (funcall join reference-node)))))
    (if (not (null alias))
	(list* alias reference-node args)
	args)))

;; Selection

(defclass selection ()
  ((joining :initarg :joining
	    :reader joining-of)))

(defclass simple-selection (selection)
  ((alias :initarg :alias
	  :reader alias-of)))

(defclass expression-selection (simple-selection)
  ((expression :initarg :expression
	       :reader expression-of)))

(defclass property-selection (simple-selection)
  ((property-slot :initarg :property-slot
		  :reader property-slot-of)))

(defclass class-selection (selection)
  ((subclass-nodes :reader subclass-nodes-of)))

(defmethod initialize-instance :after ((instance class-selection)
				       &key class-mapping &allow-other-keys)
  (with-slots (subclass-nodes subclass-list)
      instance
    (setf subclass-nodes
	  (mapcar #'(lambda (subclass-mapping)
		      (make-instance 'subclass-node
				     :class-mapping (get-class-mapping
						     (reference-class-of subclass-mapping))
				     :foreign-key (foreign-key-of subclass-mapping)
				     :class-selection instance))
		  (subclass-mappings-of class-mapping)))))

(defclass subclass-node (class-selection concrete-class-node)
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)
   (class-selection :initarg :class-selection ;; parent-node
		    :reader class-selection-of)))

(defclass root-class-selection (class-selection)
  ((subclass-node-columns :reader subclass-node-columns-of)
   (concrete-class-node :initarg :concrete-class-node
			:reader concrete-class-node-of)
   (subclass-list :reader subclass-list-of)
   (class-nodes :reader class-nodes-of)))

(defun get-subclass-node (root-class-selection class-name)
  (find (get-class-mapping class-name)
	(subclass-list-of root-class-selection)
	:key #'class-mapping-of))

(defun compute-subclass-list (subclass-nodes subclass-list)
  (let ((class-nodes
	 (set-difference subclass-nodes subclass-list)))
    (reduce #'compute-subclass-list class-nodes
	    :from-end t
	    :initial-value (append class-nodes subclass-list))))

(defun compute-class-nodes (subclass-selection ignored-class-mappings)
  (let ((subclass-nodes
	 (remove-if #'(lambda (subclass-node)
			(find (class-mapping-of class-node)
			      ignored-class-mappings))
		    (subclass-nodes-of class-node))))
    (append
     (compute-subclass-nodes (reduce #'list* subclass-nodes
				     :key #'class-mapping-of
				     :initial-value ignored-class-mappings)
			     subclass-nodes)
     (precedence-list-of subclass-selection))))

(defun compute-subclass-nodes (subclass-nodes ignored-class-mappings)
  (reduce #'append subclass-nodes
	  :key #'(lambda (class-node)
		   (ensure-class-nodes class-node
				       ignored-subclass-mappings))))

(defun compute-node-columns (class-node)
  (reduce #'(lambda (column-name result)
	      (acons column-name (make-alias column-name) result))
	  (columns-of (class-mapping-of class-node))
	  :from-end t
	  :initial-value nil))

(defmethod initialize-instance :after ((instance root-class-selection)
				       &key concrete-class-node
					 &allow-other-keys)
  (let ((precedence-list
	 (precedence-list-of concrete-class-node)))
    (with-slots (subclass-list class-nodes subclass-node-columns)
	instance
      (setf subclass-list
	    (compute-subclass-list
	     (subclass-nodes-of instance)
	     (list concrete-class-node)))
      (setf class-nodes
	    (compute-subclass-nodes
	     (subclass-nodes-of instance) precedence-list))
      (setf subclass-node-columns ;; alist by class-node (for loading)
	    (reduce #'(lambda (result class-node)
			(acons class-node
			       (compute-node-columns class-node)
			       result))
		    (append class-nodes precedence-list)
		    :initial-value nil)))))

;;; Fetching

(defclass reference-selection (root-class-selection)
  ((reference-slot :initarg :reference-slot
		   :reader reference-slot)
   (recursive-node :initarg :recursive-node
		   :reader recursive-node-of)
   (class-selection :initarg :class-selection
		    :reader class-selection-of)))

(defun get-reference-slot (concrete-class-node reader)
  (let* ((class-mapping
	  (class-mapping-of concrete-class-node))
	 (class-name
	  (class-name-of class-mapping))
	 (slot-name
	  (get-slot-name
	   (find-class class-name) reader)))
    (or (find slot-name (reference-slots-of concrete-class-node)
	      :key #'(lambda (reference-slot)
		       (slot-name-of
			(reference-mapping-of reference-slot))))
	(error "Reference ~a of class ~a not found"
	       slot-name class-name))))

(defun fetch (class-selection reader &key subclass-name fetch recursive)
  (let* ((class-node
	  (if (not (null subclass-name))
	      (get-subclass-node class-selection subclass-name)
	      (concrete-class-node-of class-selection)))
	 (reference-slot
	  (get-reference-slot class-node reader))
	 (reference-selection
	  (make-instance 'reference-selection ;; one-to-many/many-to-one?
			 :concrete-class-node  (make-instance 'concrete-class-node
							      :class-mapping class-mapping)
			 :class-mapping (get-class-mapping
					 (reference-class-of
					  (reference-mapping-of reference-slot)))
			 :class-selection class-selection
			 :recursive-selection recursive
			 :reference-slot reference-slot
			 :class-node class-node)))
    (list* reference-selection
	   (when (not (null fetch))
	     (multiple-value-list
	      (funcall fetch reference-selection))))))
