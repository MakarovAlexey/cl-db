(in-package #:cl-db)

(defclass root ()
  ((class-nodes :initform (make-hash-table)
		:reader class-nodes)
   (precedence-list :initarg :precedence-list
		    :reader precedence-list-of)
   (property-slots :reader properte-slots-of)
   (reference-slots :reader reference-slots-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

;;;; path is inverted path, from end to begin (root mapping)
(defclass mapped-slot ()
  ((path :initarg :path
	 :reader path-of)
   (root :initarg :root
	 :reader root-of)))

(defclass property-slot (mapped-slot)
  ((property-mapping :initarg :property-mapping
		     :reader property-mapping-of)))

(defclass reference-slot (mapped-slot)
  ((reference-mapping :initarg :reference-mapping
		      :reader reference-mapping-of)))

(defclass many-to-one-slot (reference-slot)
  ())

(defclass one-to-many-slot (reference-slot)
  ())

(defun compute-precedence-list (class-mapping &optional precedence-list)
  :documentation "Inherited class-mappings in inversed topological
  order (from end to begin)"
  (reduce #'compute-precedence-list
	  (superclass-mappings-of class-mapping)
	  :key (compose #'get-class-mapping #'reference-class-of)
	  :from-end t
	  :initial-value (list* class-mapping
				(remove class-mapping
					precedence-list))))

(defun iterate-inheritance (function class-mapping &key initial-value path)
    (reduce #'(lambda (result superclass-mapping)
		(iterate-inheritance function
				     (get-class-mapping
				      (reference-class-of superclass-mapping))
				     :initial-value result
				     :path (list* superclass-mapping path)))
	    (superclass-mappings-of class-mapping)
	    :initial-value (funcall function initial-value class-mapping path)))

(defun compute-properties (root class-mapping)
  (iterate-inheritance #'(lambda (result class-mapping path)
			   (reduce #'list* (property-mappings-of class-mapping)
				   :from-end t
				   :key #'(lambda (property-mapping)
					    (make-instance 'property
							   :property-mapping property-mapping
							   :path path
							   :root root))
				   :initial-value result))
		       class-mapping))

(defun compute-references (root class-mapping)
  (iterate-inheritance #'(lambda (result class-mapping path)
			   (append result
				   (mapcar #'(lambda (reference-mapping)
					       (make-instance 'many-to-one
							      :reference-mapping reference-mapping
							      :path path
							      :root root))
					   (many-to-one-mappings-of class-mapping))
				   (mapcar #'(lambda (reference-mapping)
					       (make-instance 'one-to-many
							      :reference-mapping reference-mapping
							      :path path
							      :root root))
					   (one-to-many-mappings-of class-mapping))))
		       class-mapping))

(defmethod initialize-instance :after ((instance root) &key class-mapping)
  (with-slots (precedence-list property-slots reference-slots)
      instance
    (setf precedence-list (compute-precedence-list class-mapping))
    (setf property-slots (compute-properties class-mapping))
    (setf reference-slots (compute-references class-mapping))))

(defun make-root (class-name)
  (make-instance 'root :class-mapping (get-class-mapping class-name)))

;;; Joins

(defclass joined-reference (root)
  ((reference-slot :initarg :reference-slot
		   :reader reference-slot-of)))

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

(defun join (root reader &key alias join)
  (let* ((reference-slot
	  (find (get-slot-name
		 (find-class
		  (class-name-of
		   (class-mapping-of root))) reader)
		(reference-slots-of root)
		:key #'(lambda (reference-slot)
			 (slot-name-of
			  (reference-mapping-of reference-slot)))))
	 (reference-node
	  (make-instance 'joined-reference
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

;;;; joining

(defclass node-context ()
  ((class-nodes :initform (make-hash-table) ;; class-nodes by root
		:reader class-nodes-of)))

(defun ensure-class-node (context root class-mapping)
  (or
   (find class-mapping (gethash root (class-nodes-of context))
	 :key #'class-mapping-of)
   (make-instance 'class-node
		  :class-mapping class-mapping
		  :context context
		  :root root)))

(defclass class-node ()
  ((root :initarg :root
	 :reader root-of)
   (context :initarg :context
	    :reader context-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (direct-extension :initform (make-hash-table)
		     :reader direct-extension-of)
   (direct-inheritance :initform (make-hash-table)
		       :reader direct-inheritance-of)))

(defmethod initialize-instance :after ((instance class-node)
				       &key context root class-mapping
					 &allow-other-keys)
  (setf (gethash class-mapping (class-nodes-of root)) instance)
  (push (gethash root (class-nodes-of context)) instance))

(defclass class-inheritance ()
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)
   (superclass-node :initarg :superclass-node
		    :reader superclass-node-of)
   (subclass-node :initarg :subclass-node
		  :reader subclass-node-of)))

(defclass recursive-joining (node-context)
  ((aux-clause :initarg :aux-clause
	       :reader aux-clause)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)))

(defmethod initialize-instance :after ((instance recursive-joining)
				       &key aux-clause recursive-clause)
  (ensure-class-nodes instance aux-clause)
  (ensure-class-nodes instance recursive-clause))

(defun ensure-root-class-nodes (context root)
  )

(defun ensure-path-class-nodes (context mapped-slot)
  (let ((root (root-of mapped-slot)))
    (reduce #'(lambda (superclass-mapping class-node)
		(let ((class-mapping
		       (get-class-mapping
			(reference-class-of superclass-mapping))))
		  (add-inheritance class-node
				   (ensure-class-node context root class-mapping)
				   (foreign-key-of superclass-mapping))))
	    (path-of mapped-slot)
	    :from-end t
	    :initial-value (ensure-class-node context root
					      (class-mapping-of root)))))

(defgeneric ensure-class-nodes-aggregation (context expression))

(defmethod ensure-class-nodes-aggregation (context (expression n-ary-expression)
  (dolist (argument (arguments-of expression))
    (ensure-class-nodes-aggregation argument)))

(defmethod ensure-class-nodes-aggregation (context (expression binary-expression))
  (ensure-class-nodes-aggregation context (lhs-expression-of expression))
  (ensure-class-nodes-aggregation context (rhs-expression-of expression)))

(defmethod ensure-class-nodes-aggregation (context (expression root))
  (ensure-class-node context expression (class-mapping-of expression)))

(defmethod ensure-class-nodes-aggregation (context (expression mapped-slot))
  (ensure-path-class-nodes context expression))

(defgeneric ensure-class-nodes (context expression))

(defmethod ensure-class-nodes (context (expression n-ary-expression))
  (dolist (argument (arguments-of expression))
    (ensure-class-nodes context argument))

(defmethod ensure-class-nodes (context (expression binary-operation))
  (ensure-class-nodes context (lhs-expression-of expression))
  (ensure-class-nodes context (rhs-expression-of expression)))

(defmethod ensure-class-nodes (context (expression aggregation))
  (dolist (argument (arguments-of expression))
    (ensure-class-nodes-aggregation context argument)))

(defmethod ensure-class-nodes (context (value t))
  (declare (ignore value))) ;; for parameters (numbers, strings, etc.)

;;;; references and roots
(defmethod ensure-class-nodes :after (context (expression root))
  (ensure-root-class-nodes context expression))

(defmethod ensure-class-nodes (context (expression joined-reference))
  (ensure-path-class-nodes context (reference-slot-of expression)))

(defmethod ensure-class-nodes (context (expression property-slot))
  (ensure-path-class-nodes context expression))

(defun make-recursive-joining (aux-clause recursive-clause)
  (let ((recursive-joining
	 (make-instance 'recursive-joining
			:aux-clause aux-clause
			:recursive-clause recursive-clause)))
    (ensure-class-nodes recursive-joining aux-clause)))


    

(defun make-query (roots join &key select aux recursive where order-by
				having fetch limit offset)
  (let* ((selectors (if (listp roots)
			(mapcar #'(lambda (root)
				    (make-root root))
				roots)
			(list (make-root roots))))
	 (join-list (reduce #'append (compute-clause join selectors)
			    :initial-value selectors))
	 (recursive-clause (compute-clause recursive join-list))
	 (aux-clause (compute-clause aux join-list)))
    (make-joining selectors 
    
	 (select-items (compute-clause select join-list selectors))
	 
	 (order-by-clause (compute-clause order-by join-list))
	 (having-clause (compute-clause having join-list))
	 (where-clause (compute-clause where join-list))
	 
	 (joining (make-joining (append select-items where-clause
					having-clause order-by-clause
					aux-clause recursive-clause)
				aux-clause recursive-clause))
	 (select-list (mapcar #'(lambda (select-item)
				  (select-item select-item joining))
			      select-items))
	 (reference-fetchings
	  (when (not (null fetch))
	    (reduce #'append (multiple-value-list
			      (apply fetch select-list)))))
	 (recursive-fetch-references
	  (remove-if #'null reference-fetchings
		     :key #'recursive-node-of))
	 (selection
	  (make-selection joining
			  (or limit offset recursive-fetch-references)
			  select-list where-clause having-clause
			  order-by-clause limit offset)))
    (ensure-fetching selection
		     reference-fetchings
		     recursive-fetch-references)))

(defun db-read (roots &key join aux recursive where order-by having
			select fetch singlep offset limit transform)
  (declare (ignore transform singlep))
  (make-query roots join
	      :aux aux
	      :recursive recursive
	      :select select
	      :where where
	      :order-by order-by
	      :having having
	      :offset offset
	      :limit limit
	      :fetch fetch))






























(defclass joining ()
  ((roots :initarg :roots)))

(defclass recursive-joining (query-context)
  ((table-aliases :reader table-aliases-of)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)
   (aux-clause :initarg :aux-clause
	       :reader aux-clause)
   (node-columns :initform (make-hash-table)
		 :reader column-aliases)))

(defun make-joining (roots aux-clause recursive-clause)
  (if (not (null recursive-clause))
      (make-instance 'recursive-joining
		     :roots roots
		     :aux-clause aux-clause
		     :recursive-clause recursive-clause)
      (make-instance 'joining :roots roots)))





(defun ensure-class-node (class-node from-clause)
  (if (not (find class-node from-clause))
      (list* class-node from-clause)
      from-clause))

(defgeneric append-path-node (from-clause class-node))

(defmethod append-path-node (from-clause (superclass-node superclass-node))
  (append-path-node
   (ensure-class-node superclass-node from-clause)
   (class-node-of superclass-node)))

(defmethod append-path-node (from-clause (root-node root-node))
  (ensure-class-node root-node from-clause))

(defun append-class-nodes (from-clause class-node)
  (reduce #'ensure-class-node
	  (precedence-list-of class-node)
	  :from-end t
	  :initial-value (ensure-class-node class-node from-clause)))

(defun append-property-nodes (from-clause property-slot)
  (append-path-node from-clause (class-node-of property-slot)))

(defgeneric append-aggregation-nodes (from-clause expression))

(defmethod append-aggregation-nodes (from-clause (expression expression))
  (reduce #'append-aggregation-nodes (arguments-of expression)
	  :initial-value from-clause))

(defmethod append-aggregation-nodes (from-clause (expression property-slot))
  (append-property-nodes from-clause expression))

(defmethod append-aggregation-nodes (from-clause (expression root-class-selection))
  (ensure-class-node
   (concrete-class-node-of expression) from-clause))

(defmethod append-aggregation-nodes (from-clause (expression reference-node))
  (ensure-class-node
   (concrete-class-node-of expression)
   (append-path-node from-clause
		     (class-node-of
		      (reference-slot-of expression)))))



(defmethod initialize-instance :after ((instance joining)
				       &key expressions &allow-other-keys)
  (with-slots (from-clause table-aliases)
      instance
    (setf from-clause
	  (reduce #'append-joining-nodes
		  expressions :initial-value nil))
    (setf table-aliases
	  (reduce #'(lambda (result class-node)
		      (acons class-node
			     (make-alias
			      (class-name-of
			       (class-mapping-of class-node)))
			     result))
		  (from-clause-of instance)
		  :initial-value nil))))

(defclass column-selection ()
  ((column-name :initarg :column-name
		:reader column-name-of)
   (class-node :initarg :class-node
	       :reader class-node-of)
   (alias :initarg :alias
	  :reader alias-of)))

(defun ensure-column (selected-columns column-name class-node)
  (if (find-if-not #'(lambda (selected-column)
		       (and (eq (class-node-of selected-column)
				class-node)
			    (eq (column-name-of selected-column)
				column-name)))
		   selected-columns)
      (list* (make-instance 'column-selection
			    :column-name column-name
			    :class-node class-node
			    :alias (make-alias column-name))
	     selected-columns)
      selected-columns))

(defun compute-class-node-columns (select-columns class-node)
  (reduce #'(lambda (result column-name)
	      (ensure-column result column-name class-node))
	  (columns-of
	   (class-mapping-of class-node))
	  :initial-value select-columns))

(defgeneric compute-joining-column-aliases (expression result))

(defmethod compute-joining-column-aliases ((root-node root-node) result)
  (reduce #'(lambda (class-node result)
	      (acons class-node
		     (compute-class-node-columns result class-node)
		     result))
	  (precedence-list-of root-node)
	  :from-end t
	  :initial-value nil))

(defmethod compute-joining-column-aliases ((property-slot property-slot) result)
  (let ((property-mapping (property-mapping-of property-slot)))
    (ensure-column result
		   (column-of property-mapping)
		   (class-node-of property-slot))))

(defmethod compute-joining-column-aliases ((expression n-ary-expression) result)
  (select-columns (arguments-of expression) result))

(defmethod compute-joining-column-aliases ((expression binary-operation) result)
  (compute-joining-column-aliases
   (rhs-expression-of expression)
   (compute-joining-column-aliases
    (lhs-expression-of expression) result)))

(defmethod compute-joining-column-aliases ((expression recursive-class-node) result)
  (compute-joining-column-aliases
   (rhs-expression-of expression)
   (compute-joining-column-aliases
    (lhs-expression-of expression) result)))

(defun select-columns (expressions &optional (selected-columns nil))
  (reduce #'compute-joining-column-aliases expressions
	  :initial-value selected-columns
	  :from-end t))

(defmethod initialize-instance :after ((instance recursive-joining)
				       &key expressions &allow-other-keys)
  (with-slots (column-aliases)
      instance
    (setf column-aliases (select-columns expressions))))
