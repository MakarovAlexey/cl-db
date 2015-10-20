(in-package #:cl-db)

(defclass root ()
  ((class-nodes :initform (make-hash-table)
		:reader class-nodes)
   (superclass-paths :initarg :superclass-paths
		    :reader superclass-paths-of)
   (property-slots :reader properte-slots-of)
   (reference-slots :reader reference-slots-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

(defun get-superclass-path (root-1 root-2)
  (rest
   (first
    (intersection
     (superclass-paths-of root-1)
     (superclass-paths-of root-2)
     :key #'first))))

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

(defun compute-superclass-paths (class-mapping &optional path)
  :documentation "Inherited class-mappings in inverse topological
  order (from end to begin)"
  (acons class-mapping path
	 (reduce #'append
		 (superclass-mappings-of class-mapping)
		 :key #'(lambda (superclass-mapping)
			  (compute-precedence-list
			   (get-class-mapping
			    (reference-class-of superclass-mapping))
			   (list* superclass-mapping path)))
		 :from-end t
		 :initial-value nil)))

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
    (setf superclass-paths (compute-superclass-paths class-mapping))
    (setf property-slots (compute-properties instance class-mapping))
    (setf reference-slots (compute-references instance class-mapping))))

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

(defclass recursive ()
  ((root :initarg :root
	 :reader root-of)))

(defun recursive (root)
  (make-instance 'recursive :root root))

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

(defun get-root-class-nodes (root context)
  (gethash root (class-nodes-of context)))

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

(defclass query ()
  ((previous-contexts :initform (make-hash-table)
		      :reader previous-contexts-of)))

(defclass context ()
  ((query :initarg :query
	  :reader query-of)
   (class-nodes :initform (make-hash-table) ;; class-nodes by root
		:reader class-nodes-of)
   (class-node-columns :initform (make-hash-table)
		       :reader class-node-columns-of)
   (expression-aliases :initform (make-hash-table) ;; columns and expressions by aliases
		       :accessor expression-aliases-of) ;; class-node => (list (column . alias)); expression => alias
   (aux-clause :initarg :aux-clause
	       :reader aux-clause-of)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)
   (select-list :initarg :select-list
		:reader select-list-of)
   (where-clause :initarg :where-clause
		 :reader where-clause-of)
   (group-by-present-p :initform nil
		       :accessor group-by-clause-of)
   (order-by-clause :initarg :order-by-clause
		    :reader order-by-clause-of)
   (having-clause :initarg :having-clause
		  :reader having-clause-of)
   (fetch-clause :initarg :fetch-clase
		 :reader fetch-clause-of)))

(defclass column () ;;(select-item)
  ((name :initarg :name
	 :reader name-of)
   (correlation :initarg :correlation
		:reader correlation-of)))

(defun ensure-external-column-selection (context class-node column-name)
  (make-instance 'column
		 :name (select-column context class-node column-name)
		 :correlation context)))

(defun ensure-column (context class-node column-name)
  (if (not (eq context (context-of class-node)))
      (ensure-external-column-selection
       (previous-context context) class-node column-name)
      (make-instance 'column
		     :correlation class-node
		     :name column-name)))

(defun ensure-direct-inheritance (context root class-node
				  superclass-mapping)
  (let ((class-mapping
	 (get-class-mapping
	  (reference-class-of superclass-mapping))))
    (or
     (get-inheritance class-mapping class-node)
     (make-instance 'direct-inheritance
		    :superclass-node (ensure-class-node context root class-mapping)
		    :foreign-key (mapcar #'(lambda (column-name)
					     (ensure-column context
							    class-node
							    column-name))
					 (foreign-key-of superclass-mapping))
		    :subclass-node class-node))))

(defun ensure-alias (expression context)
  (ensure-gethash expression (expression-aliases-of context)
		  (make-alias (name-of expression))))

(defun select-column (context class-node column-name)
  (let ((columns
	 (gethash class-node (class-node-columns-of context))))
    (ensure-alias
     (rest
      (or (assoc column-name columns)
	  (setf (gethash class-node (class-node-columns-of context))
		(acons column-name
		       (ensure-column context class-node column-name)
		       columns))))
     context)))

(defun select-all-inheritance-class-nodes (context root class-node)
  (dolist (superclass-mapping (superclass-mappings-of class-mapping))
    (select-all-inheritance-class-nodes context root
					(superclass-node-of
					 (ensure-direct-inheritance context
								    root
								    class-node
								    superclass-mapping))))
    (dolist (column-name (columns-of class-mapping))
      (select-column context class-node column-name)))

(defun select-class (context root)
  (select-all-inheritance-class-nodes context expression
				      (ensure-class-node context root
							 (class-mapping-of root))))

(defun ensure-path-class-nodes (context root class-mapping path)
  (reduce #'(lambda (class-node superclass-mapping)
	      (superclass-node-of
	       (ensure-direct-inheritance context root class-node
					  superclass-mapping)))
	  path
	  :from-end t
	  :initial-value (ensure-class-node context root class-mapping)))

(defun parse-class-equality (context lhs-root rhs-root)
  (ensure-path-class-nodes context rhs-root
			   (class-mapping-of rhs-root)
			   (get-superclass-path rhs-root lhs-root))
  (ensure-path-class-nodes context lhs-root
			   (class-mapping-of lhs-root)
			   (get-superclass-path lhs-root rhs-root)))

(defun ensure-slot-path (context mapped-slot)
  (ensure-path-class-nodes context (root-of mapped-slot)
			   (class-mapping-of mapped-slot)
			   (path-of mapped-slot)))

(defun select-property (context property-slot)
  (select-column context (ensure-slot-path context property-slot)
		 (column-of (slot-mapping-of property-slot))))

(defun ensure-property (context property-slot)
  (ensure-column context (ensure-slot-path context expression)
		 (column-of (slot-mapping-of property-slot))))

(defun ensure-root-primary-key (context root)
  (let ((class-mapping
	 (class-mapping-of root))
	(class-node
	 (ensure-class-node context root class-mapping)))
    (dolist (column-name (primary-key-of class-mapping))
      (ensure-column context class-node column-name))))

;; subclasses
;; ensure-reference-foreign-key

(defgeneric parse-recursive-expression (context expression))

(defmethod parse-recursove-expression (context expression))

(defmethod parse-recursive-expression :after (context (expression property-slot))
  (select-property context expression))

(defmethod parse-recursive-expression :after (context (expression root))
  (ensure-root-primary-key context expression))

(defmethod parse-recursive-expression :after (context (expression joined-reference))
  (ensure-slot-path context (reference-slot-of expression)))

(defgeneric parse-expression (context expression))

(defmethod parse-expression (context expression))

(defmethod parse-expression :after (context (expression n-ary-expression))
  (dolist (expression (arguments-of expression))
    (parse-expression context expression)))

(defmethod parse-expression :after (context (expression binary-operation))
  (parse-expression context (lhs-expression-of expression))
  (parse-expression context (rhs-expression-of expression)))

(defmethod parse-expression :after (context (expression class-equality))
  (parse-class-equality context (lhs-root-of expression)
			(rhs-root-of expression)))

(defmethod parse-expression :after (context (expression root))
  (ensure-class-node context expression (class-mapping-of expression)))

(defmethod parse-expression :after (context (expression joined-reference))
  (ensure-slot-path context (reference-slot-of expression)))

(defmethod parse-expression :after (context (expression property-slot))
  (ensure-property context expression))

(defgeneric parse-aggregate-expression (context expression))
;; :after
(defmethod parse-aggregate-expression (context (expression aggregate-expression))
  (error "Aggregate expression ~a does not itself contain an aggregate expression" expression))

(defmethod parse-aggregate-expression :after (context (expression n-ary-expression))
  (dolist (argument (arguments-of expression))
    (parse-aggregate-expression argument)))

(defmethod parse-aggregate-expression :after (context (expression binary-expression))
  (parse-aggregate-expression context (lhs-expression-of expression))
  (parse-aggregate-expression context (rhs-expression-of expression)))

(defmethod parse-aggregate-expression :after (context (expression class-equality))
  (parse-class-equality context (lhs-root-of expression)
			(rhs-root-of expression)))

(defmethod parse-aggregate-expression :after (context (expression root))
  (ensure-class-node context expression (class-mapping-of expression)))

(defmethod parse-aggregate-expression :after (context (expression joined-reference))
  (ensure-slot-path context (reference-slot-of expression)))

(defmethod parse-aggregate-expression :after (context (expression property-slot))
  (ensure-property context expression))

(defgeneric parse-select-item (context expression))

(defmethod parse-select-item (context expression))

(defmethod parse-select-item :after (context (expression n-ary-expression))
  (dolist (expression (arguments-of expression))
    (parse-select-item context expression)))

(defmethod parse-select-item :after (context (expression binary-operation))
  (parse-select-item context (lhs-expression-of expression))
  (parse-select-item context (rhs-expression-of expression)))

(defmethod parse-select-item :after (context (expression class-equality))
  (parse-class-equality context (lhs-root-of expression)
			(rhs-root-of expression)))

(defmethod parse-select-item :after (context (expression root))
  (ensure-class-node context expression (class-mapping-of expression)))

(defmethod parse-select-item :after (context (expression joined-reference))
  (ensure-slot-path context (reference-slot-of expression)))

(defmethod parse-select-item :after (context (expression property-slot))
  (ensure-property context expression))

(defmethod parse-select-item :after (context (expression aggregate-expression))
  (when (not (aggregation-present-p context))
    (setf (aggregation-present-p context) t))
  (dolist (expression (arguments-of expression))
    (parse-aggregate-expression context expression)))

(defgeneric parse-select-list (context expression))

(defmethod parse-select-list (context (expression t)))

(defmethod parse-select-list :after (context (expression root))
  (select-root context expression))

(defmethod parse-select-list :after (context (expression joined-reference))
  (ensure-slot-path context (reference-slot-of expression)))

(defmethod parse-select-list :after (context (expression property-slot))
  (select-property context expression))

(defmethod parse-select-list :after (context (expression expression))
  (parse-select-item context expression)
  (setf (gethash expression (expression-aliases-of context))
	(make-alias (name-of expression))))

;; (defgeneric parse-order-by-clause (context expression))

;; (defmethod parse-order-by-clause (context expression))

;; (defmethod parse-order-by-clause :after (context (expression ascending-order))
;;   (parse-order-by-clause context (expression-of expressoin)))

;; (defmethod parse-order-by-clause :after (context (expression descending-order))
;;   (parse-order-by-clause context (expression-of expressoin)))

(defgeneric parse-fetch-clause (context fetch-reference))

;; parse of order-by-clause and having-clause not needed becose this
;; exoressins compute in select-list
(defmethod initialize-instance ((instance context)
				&key previous-context query aux-clause
				  recursive-clause select-list
				  where-clause order-by-clause
				  having-clause fetch-clause 
				  &allow-other-keys)
  (when (not null previous-context)
    (setf (gethash instance (previous-contexts-of query))
	  previous-context))
  (dolist (expression aux-clause)
    (parse-expression instance expression))
  (dolist (expression recursive-clause)
    (parse-expression instance expression))
  (dolist (expression select-list)
    (parse-select-list instance expression))
  (dolist (expression where-clause)
    (parse-expression instance expression))
  (dolist (expression fetch-clause)
    (parse-fetch-clause instance expression)))

(defun previous-context (context)
  (gethash context (previous-contexts-of (query-of context))))

(defun make-context (query name &key previous-context aux recursive
				  where order-by having select
				  offset limit fetch)
  (make-instance 'context
		 :name name
		 :query query
		 :previous-context previous-context
		 :aux-clause aux
		 :reacursive-clause recursive
		 :select-list select
		 :where-clause where
		 :order-by-clause order-by
		 :having-clause having
		 :limit limit
		 :offset offset
		 :fetch fetch))

(defun ensure-recursive-joining (query aux-clause recursive-clause)
  (when (not (null recursive-clause))
    (make-context query "recursive_joining"
		  :aux aux-clause
		  :recursive recursive-clause)))

(defun make-selection (query previous-context select-list where-clause
		       order-by-clause having-clause limit offset
		       fetch-clause)
  (let ((recursive-fetch
	 (remove-if #'null fetch-clause :key #'recursive-node-of)))
    (if (or (not (null recursive-clause))
	    (and (not (null fetch-clause))
		 (or (not (null offset))
		     (not (null limit)))))
	(let ((selection
	       (make-context query "selection"
			     :previous-context previous-context
			     :select select-list
			     :where where-clause
			     :order-by order-by-clause
			     :having having-clause
			     :offset offset
			     :limit limit)))
	  (if (not (null recursive-clause))
	      (make-context query "main" :previous-context
			    (make-context query "fetching"
					  :previous-context selection
					  :fetch fetch
					  :recursive recursive-fetch)
			    :select select-list
			    :fetch fetch
			    :order-by order-by-clause)
	      (make-context query "main"
			    :previous-context selection
			    :select fetch-select-list
			    :order-by order-by-clause))
	  (make-context query "selection"
			:previous-context previous-context
			:select (append select-list fetch-clause)
			:where where-clause
			:order-by order-by-clause
			:having having-clause
			:offset offset
			:limit limit)))))

(defun make-query (class-names join &key select aux recursive where
				      order-by having fetch limit offset)
  (let* ((selectors (if (listp class-names)
			(mapcar #'(lambda (class-name)
				    (make-root class-name))
				class-names)
			(list (make-root class-names))))
	 (join-list (reduce #'append (compute-clause join selectors)
			    :initial-value selectors))
	 (aux-clause (compute-clause aux join-list))
	 (recursive-clause (compute-clause recursive join-list))
	 (select-list (compute-clause select join-list selectors))
	 (where-clause (compute-clause where join-list))
	 (order-by-clause (compute-clause order-by join-list))
	 (having-clause (compute-clause having join-list))
	 (fetch-clause (when (not (null fetch))
			 (reduce #'append (multiple-value-list
					   (apply fetch select-list)))))
	 (query (make-instance 'query)))
    (make-selection query
		    (ensure-recursive-joining query aux-clause recursive-clause)
		    select-list where-clause order-by-clause
		    having-clause limit offset fetch-clause)))

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

