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
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (path :initarg :path
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
  :documentation "Inherited class-mappings in inverse topological
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
							   :class-mapping class-mapping
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
							      :class-mapping class-mapping
							      :reference-mapping reference-mapping
							      :path path
							      :root root))
					   (many-to-one-mappings-of class-mapping))
				   (mapcar #'(lambda (reference-mapping)
					       (make-instance 'one-to-many
							      :class-mapping class-mapping
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

(defclass context ()
  ((query :initarg :query
	  :reader query-of)
   (class-nodes :initform (make-hash-table) ;; class-nodes by root
		:reader class-nodes-of)
   (select-list :initform (make-hash-table) ;; columns and expressions by aliases
		:accessor select-list-of)))

(defclass clause ()
  ((context :initarg :context
	    :reader context-of)
   (expressions :initarg :expressions
		:reader expressions-of)))

(defclass aux-clause (clause)
  ())

(defclass recursive-clause (clause)
  ())

(defclass select-list (clause)
  ())

(defclass where-clause (clause)
  ())

(defclass order-by-clause (clause)
  ())

(defclass having-clause (clause)
  ())

(defclass fetch-clause (clause)
  ())

(defclass recursive-joining (context)
  ((aux-clause :initarg :aux-clause
	       :reader aux-clause)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)))

;; context is query or their auxliary part. FROM clause of query is
;; (list* (previous-context-of context) (class-nodes-of context))

(defun get-column-alias (column-name class-node-select-item)
  (gethash column-name (columns-of class-node-select-item)))

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

(defclass direct-inheritance ()
  ((superclass-node :initarg :superclass-node
		    :reader superclass-node-of)
   (subclass-node :initarg :subclass-node
		  :reader subclass-node-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defun get-inheritance (class-mapping class-node)
  (gethash class-mapping (direct-inheritance-of class-node)))

(defun get-extension (class-mapping class-node)
  (gethash class-mapping (direct-extension-of class-node)))

(defclass recursive ()
  ((class-node :initarg :class-node
	       :reader class-node-of)))

(defun recursive (class-node)
  (make-instance 'recursive :class-node class-node))

(defmethod initialize-instance :after ((instance class-node)
				       &key context root class-mapping
					 &allow-other-keys)
  (setf (gethash class-mapping (class-nodes-of root)) instance)
  (push (gethash root (class-nodes-of context)) instance))

(defun ensure-class-node (context root class-mapping)
  (or
   (find class-mapping (gethash root (class-nodes-of context))
	 :key #'class-mapping-of)
   (make-instance 'class-node
		  :class-mapping class-mapping
		  :context context
		  :root root)))

(defclass select-item ()
  ((alias :initarg :alias
	  :reader alias-of)))

(defclass column (select-item)
  ((name :initarg :name :reader name-of)
   (table-reference :initarg :table-reference
		    :reader table-reference-of)))

(defun ensure-column-selected (context class-node column-name)
  (multiple-value-bind (column presentp)
      (get-column-selection context class-node column-name)
    (if (not presentp)
	(push (ensure-column context class-node column-name)
	      (gethash class-node (select-list-of context)))
	column)))

(defun ensure-extenal-column-selection (context class-node column-name)
  (make-instance 'context-column-selection
		 :column (ensure-column-selected context class-node
						 column-name)
		 :context context))

(defun ensure-column (context class-node column-name)
  (if (not (eq context (context-of class-node)))
      (ensure-external-column-selection
       (previous-context context) class-node column-name)
      (make-instance 'column-expression
		     :class-node class-node
		     :column-name column-name)))

(defun ensure-inheritance (context root class-node superclass-mapping)
  (let ((class-mapping
	 (get-class-mapping
	  (reference-class-of superclass-mapping))))
    (multiple-value-bind (direct-inheritance presentp)
	(get-inheritance class-mapping class-node)
      (if (not presentp)
	  (make-instance 'direct-inheritance
			 :superclass-node (ensure-class-node context root class-mapping)
			 :foreign-key (mapcar #'(lambda (column-name)
						  (ensure-column context class-node column-name))
					      (foreign-key-of superclass-mapping))
			 :subclass-node class-node)
	  direct-inheritance))))

(defun ensure-all-inheritance-class-nodes (context root class-node)
  (dolist (superclass-mapping
	    (superclass-mappings-of (class-mapping-of class-node))
	   class-node)
    (ensure-all-inheritance-class-nodes context root
					(superclass-node-of
					 (ensure-inheritance context
							     root
							     class-node
							     superclass-mapping)))))

(defun ensure-path-class-nodes (context mapped-slot)
  (let ((root (root-of mapped-slot)))
    (reduce #'(lambda (superclass-mapping class-node)
		(superclass-node-of
		 (ensure-inheritance context root class-node superclass-mapping)))
	    (path-of mapped-slot)
	    :from-end t
	    :initial-value (ensure-class-node context root
					      (class-mapping-of mapped-slot)))))

(defgeneric ensure-class-nodes (context clause expression))

(defmethod ensure-class-nodes (context clause (expression n-ary-expression))
  (dolist (argument (arguments-of expression))
    (ensure-class-nodes context argument))))

(defmethod ensure-class-nodes (context clause (expression binary-operation))
  (ensure-class-nodes context (lhs-expression-of expression))
  (ensure-class-nodes context (rhs-expression-of expression)))

(defmethod ensure-class-nodes (context clause (value t))
  (declare (ignore value))) ;; for parameters (numbers, strings, etc.)

(defmethod ensure-class-nodes (context clause (expression joined-reference))
  (ensure-path-class-nodes context (reference-slot-of expression)))

(defmethod ensure-class-nodes (context clause (expression property-slot))
  (ensure-path-class-nodes context expression))

(defmethod initialize-instance :after ((instance recursive-joining)
				       &key aux-clause recursive-clause)
  (ensure-class-nodes instance aux-clause)
  (ensure-class-nodes instance recursive-clause))

(defun make-recursive-joining (aux-clause recursive-clause)
  (make-instance 'recursive-joining
		 :aux-clause aux-clause
		 :recursive-clause recursive-clause))

(defclass selection (context)
  ((select-list :initarg :select-list
		:reader select-list-of)
   (where-clause :initarg :where-clause
		 :reader where-clause-of)
   (having-clause :initarg :having-clause
		  :reader having-clause-of)
   (order-by-clause :initarg :order-by-clause
		    :reader order-by-clause-of)
   (limit :initarg :limit
	  :reader limit-of)
   (offset :initarg :offset
	   :reader offset-of)))

(defmethod initialize-instance :after ((instance selection)
				       &key select-list where-clause
					 having-clause order-by-clause
					 &allow-other-keys)
  (dolist (expression (expressions-of select-list))
    (ensure-class-nodes instance select-list expression))
  (dolist (expression (expressions-of where-clause))
    (ensure-class-nodes instance where-clause expression))
  (dolist (expression (expressions-of having-clause))
    (ensure-class-nodes instance having-clause expression))
  (dolist (expression (expressions-of order-by-clause))
    (ensure-class-nodes instance order-by-clause expression)))

;; выбрать колонки по class-nodes (составить sql-select-list)
(defun make-selection (select-list where-clause having-clause
		       order-by-clause limit offset)
  (make-instance 'selection
		 :select-list select-list
		 :where-clause where-clause
		 :having-clause having-clause
		 :order-by-clause order-by-clause
		 :offset offset
		 :limit limit))
  
;;;; references and roots
(defmethod ensure-class-nodes :after (context (clause select-list) (expression root))
  (ensure-all-inheritance-class-nodes context expression
				      (ensure-class-node context root
							 (class-mapping-of root))))

(defmethod ensure-class-nodes (context (clause select-list) (expression aggregation))
  (dolist (argument (arguments-of expression))
    (ensure-class-nodes-aggregation context argument)))

(defgeneric ensure-class-nodes-aggregation (context expression))

(defmethod ensure-class-nodes-aggregation (context (expression aggregate-expression))
  (dolist (argument (arguments-of expression))
    (ensure-class-nodes-aggregation argument)))

(defmethod ensure-class-nodes-aggregation (context (expression binary-expression))
  (ensure-class-nodes-aggregation context (lhs-expression-of expression))
  (ensure-class-nodes-aggregation context (rhs-expression-of expression)))

(defmethod ensure-class-nodes-aggregation (context (expression root))
  (ensure-class-node context expression (class-mapping-of expression)))

(defmethod ensure-class-nodes-aggregation (context (expression mapped-slot))
  (ensure-path-class-nodes context expression))

(defclass fetching ()
  ((fetch-clause :initarg :fetch-clause
		 :reader fetch-clause-of)
   (recursive-fetch-clause :reader recursive-fetch-clause-of)))

(defmethod initialize-instance :after ((instance fetching)
				       &key fetch-clause &allow-other-keys)
  (setf (slot-value instance 'recursive-fetch-clause)
	(remove-if #'null reference-fetchings
		   :key #'recursive-node-of)))



(defun make-query (class-names join &key select aux recursive where
				      order-by having fetch limit offset)
  (let* ((selectors (if (listp class-names)
			(mapcar #'(lambda (class-name)
				    (make-root class-name))
				class-names)
			(list (make-root class-names))))
	 (join-list (reduce #'append (compute-clause join selectors)
			    :initial-value selectors)))
    (
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
