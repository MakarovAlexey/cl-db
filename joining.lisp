(in-package #:cl-db)

(defclass expression ()
  ())

(defclass binary-expression (expression)
  ((lhs-expression :initarg :lhs :reader lhs-expression-of)
   (rhs-expression :initarg :rhs :reader rhs-expression-of)))

(defclass n-ary-expression (expression)
  ((arguments :initarg :arguments
	      :reader arguments-of)))

(defclass sql-function (n-ary-expression)
  ())

(defclass rdbms-sql-function (sql-function)
  ((name :initarg :name :reader name-of)))

(defclass aggregate-expression (sql-function)
  ())

(defclass rdbms-aggeregation (aggregate-expression)
  ((name :initarg :name :reader name-of)))

(defclass binary-operator-extended (n-ary-expression)
  ())

(defclass sort-direction (expression)
  ((expression :initarg :expression
	       :reader expression-of)))

(defclass ascending (sort-direction)
  ())

(defclass descending (sort-direction)
  ())

(defmacro define-binary-operation (name)
  `(defclass ,name (binary-expression)
     ()))

(defmacro define-binary-operator-extended (name)
  `(defclass ,name (binary-operator-extended)
     ()))

(defmacro define-sql-function (name)
  `(defclass ,name (sql-function)
     ()))

(defmacro define-aggregate-function (name)
  `(defclass ,name (aggregation)
     ()))

(define-binary-operation less-than)

(define-binary-operation greater-than)

(define-binary-operation less-than-or-equal)

(define-binary-operation greater-than-or-equal)

(define-binary-operation equality)

(define-binary-operation not-equal)

(define-binary-operation like)

(define-binary-operation is-null)

(define-binary-operation is-not-null)

(define-binary-operation is-true)

(define-binary-operation is-not-true)

(define-binary-operation is-false)

(define-binary-operation is-not-false)

(define-binary-operator-extended conjunction)

(define-binary-operator-extended disjunction)

(define-binary-operator-extended addition)

(define-binary-operator-extended subtraction)

(define-binary-operator-extended multiplication)

(define-binary-operator-extended division)

(define-sql-function sql-abs)

(define-sql-function sql-exp)

(define-sql-function sql-floor)

(define-sql-function sql-log)

(define-sql-function sql-mod)

(define-sql-function sql-power)

(define-sql-function sql-round)

(define-sql-function sql-sqrt)

(define-sql-function sql-trunc)

(define-sql-function sql-acos)

(define-sql-function sql-asin)

(define-sql-function sql-atan)

(define-sql-function sql-cos)

(define-sql-function sql-sin)

(define-sql-function sql-tan)

(define-aggregate-function avg)

(define-aggregate-function sql-count)

(define-aggregate-function sql-every)

(define-aggregate-function sql-max)

(define-aggregate-function sql-min)

(define-aggregate-function sql-sum)

(defun disjunction (restriction &rest more-restrictions)
  (make-instance 'disjunction
		 :arguments (list* restriction more-restrictions)))

(defun conjunction (restriction &rest more-restrictions)
  (make-instance 'conjunction
		 :arguments (list* restriction more-restrictions)))

(defun restrict (property &key equal not-equal not is like not-like
			    less-than less-than-or-equal
			    more-than more-than-or-equal)
  (let ((operations
	 (list (cons 'equality equal)
	       (cons 'not-equal not-equal)
	       (cons 'not not)
	       (cons 'is is)
	       (cons 'like like)
	       (cons 'not-like not-like)
	       (cons 'less-than less-than)
	       (cons 'less-than-or-equal less-than-or-equal)
	       (cons 'more-than more-than)
	       (cons 'more-than-or-equal more-than-or-equal))))
  (make-instance 'conjunction :arguments 
		 (loop for (class-name . parameter) in operations
		    when (not (null parameter))
		    collect (make-instance class-name
					   :lhs property
					   :rhs parameter)))))

(defgeneric projection (descriptor &rest args))

(let ((projections
       (list #'+ 'addition
	     #'- 'subtracion
	     #'* 'multiplication
	     #'/ 'division
	     #'member 'memeber)))
  (defmethod projection ((descriptor function) &rest args)
    (let ((projection-name (getf projections descriptor)))
      (apply #'projection projection-name args))))

(defmethod projection ((descriptor symbol) &rest args)
  (make-instance descriptor :arguments args))

(defmethod projection ((descriptor string) &rest args)
  (make-instance 'rdbms-function :name descriptor :args args))

(defgeneric aggregation (descriptor &rest args))

(let ((aggregations
       (list #'+ 'sql-sum
	     #'min 'sql-min
	     #'max 'sql-max
	     #'every 'sql-every
	     #'count 'sql-count)))
  (defmethod aggregation ((descriptor function) &rest args)
    (let ((class-name (getf aggregations descriptor)))
      (apply #'aggregation class-name args))))

(defmethod aggregation ((descriptor symbol) &rest args)
  (make-instance descriptor :arguments args))

(defmethod aggregation ((descriptor string) &rest args)
  (make-instance 'rdbms-aggregation :name descriptor :args args))

(defun ascending (arg)
  (make-instance 'ascending :arg arg))

(defun descending (arg)
  (make-instance 'descending :arg arg))

(defgeneric property (node reader))

(defmethod property ((root-node root-node) reader)
  (let* ((class-name
	  (class-name-of (class-mapping-of root-node)))
	 (slot-name
	  (get-slot-name (find-class class-name) reader)))
    (or
     (find slot-name
	   (property-slots-of root-node)
	   :key #'(lambda (property-node)
		    (slot-name-of
		     (property-mapping-of property-node))))
     (error "Property mapping for slot-name ~a of class mapping ~a not found"
	    slot-name class-name))))

(defmethod property ((recursive-node recursive-class-node) reader)
  (property (class-node-of recursive-node) reader))

(defmethod property ((class-selection root-class-selection) reader)
  (property (concrete-class-node-of class-selection) reader))

;;;;

(defclass root ()
  ((class-nodes :initform (make-hash-table)
		:reader class-nodes)
   (superclass-paths :reader superclass-paths-of)
   (reference-slots :reader reference-slots-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (subclass-roots :initform (list)
		   :accessor subclass-roots-of)))

(defun get-superclass-path (root-1 root-2)
  (rest
   (first
    (intersection
     (superclass-paths-of root-1)
     (superclass-paths-of root-2)
     :key #'first))))

(defun get-class-node (root class-mapping)
  (gethash class-mapping (class-nodes-of root)))

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

(defun compute-references (root class-mapping)
  (iterate-inheritance #'(lambda (result class-mapping path)
			   (append result
				   (mapcar #'(lambda (reference-mapping)
					       (make-instance 'many-to-one-slot
							      :class-mapping class-mapping
							      :reference-mapping reference-mapping
							      :path path
							      :root root))
					   (many-to-one-mappings-of class-mapping))
				   (mapcar #'(lambda (reference-mapping)
					       (make-instance 'one-to-many-slot
							      :class-mapping class-mapping
							      :reference-mapping reference-mapping
							      :path path
							      :root root))
					   (one-to-many-mappings-of class-mapping))))
		       class-mapping))

(defmethod initialize-instance :after ((instance root) &key class-mapping)
  (with-slots (superclass-paths reference-slots)
      instance
    (setf superclass-paths (compute-superclass-paths class-mapping))
    (setf reference-slots (compute-references instance class-mapping))))

(defclass query-root (root)
  ((property-slots :reader properte-slots-of)
   (subclass-roots :initform (make-hash-table)
		   :reader subclass-roots-of)))

(defun get-subclass-root (class-mapping root)
  (gethash class-mapping (subclass-roots-of root)))

(defun list-subclass-roots (query-root)
  (hash-table-values (subclass-roots-of query-root)))

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

(defun make-root (class-name)
  (make-instance 'query-root :class-mapping (get-class-mapping class-name)))

(defmethod initialize-instance :after ((instance query-root) &key class-mapping)
  (with-slots (property-slots)
      instance
    (setf property-slots (compute-properties instance class-mapping))))

(defclass subclass-root (root)
  ((query-root :initarg :query-root
	       :reader query-root-of)
   (superclass-root :initarg :superclass-root
		    :reader superclass-root-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defmethod initialize-instance :after ((instance subclass-root)
				       &key superclass-root
					 class-mapping query-root)
  (push instance (subclass-roots-of superclass-root))
  (setf (gethash class-mapping
		 (subclass-roots-of query-root))
	instance))

;;; Joins

(defclass joined-reference (query-root)
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

;;;; fetching

(defclass fetched-reference (root)
  ((reference-mapping :initarg :reference-mapping
		      :reader reference-mapping-of)
   (superclass-root :initarg :superclass-root
		    :reader superclass-root-of)
   (recursive-node :initarg :recursive-node
		   :reader recursive-node-of)))

(defun fetch (root reader &key subclass-name fetch recursive)
  (let* ((root
	  (if (not (null subclass-name))
	      (get-subclass-root subclass-name root)
	      root))
	 (reference-slot
	  (get-reference-slot root reader))
	 (fetched-reference
	  (make-instance 'fetched-reference
			 :class-mapping (get-class-mapping
					 (reference-class-of
					  (reference-mapping-of reference-slot)))
			 :reference-slot reference-slot
			 :recursive-node recursive
			 :root root)))
    (list* fetched-reference
	   (when (not (null fetch))
	     (reduce #'append
		     (multiple-value-list
		      (funcall fetch fetched-reference)))))))

(defclass recursive ()
  ((root :initarg :root
	 :reader root-of)))

(defun recursive (root)
  (make-instance 'recursive :root root))

;; context is query or their auxliary part (WITH- statement). FROM
;; clause of query is (list* (previous-context-of context)
;; (class-nodes-of context))

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

(defclass direct-inheritance ()
  ((superclass-node :initarg :superclass-node
		    :reader superclass-node-of)
   (subclass-node :initarg :subclass-node
		  :reader subclass-node-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defmethod initialize-instance :after ((instance direct-inheritance)
				       &key superclass-node subclass-node)
  (setf (gethash (class-mapping-of subclass-node)
		 (direct-extension-of superclass-node))
	instance)
  (setf (gethash (class-mapping-of superclass-node)
		 (direct-inheritance-of subclass-node))
	instance))

(defun ensure-class-node (context root class-mapping)
  (multiple-value-bind (class-node presentp)
      (gethash class-mapping (class-nodes-of root))
    (if (not presentp)
	(make-instance 'class-node
		       :class-mapping class-mapping
		       :context context
		       :root root)
	class-node)))

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
		       :accessor group-by-present-p)
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
		 :correlation context))

(defun ensure-column (context class-node column-name)
  (let ((columns
	 (gethash class-node (class-node-columns-of context))))
    (rest
     (or (assoc column-name columns)
	 (setf (gethash class-node (class-node-columns-of context))
	       (acons column-name
		      (if (not (eq context (context-of class-node)))
			  (ensure-external-column-selection
			   (previous-context context) class-node column-name)
			  (make-instance 'column
					 :correlation class-node
					 :name column-name))
		      columns))))))

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
  (ensure-alias
   (ensure-column context class-node column-name) context))

(defun select-class-node (class-node context)
  (dolist (column-name (columns-of (class-mapping-of class-node)))
    (select-column context class-node column-name)))

(defun select-all-inheritance-class-nodes (context root class-node)
  (let ((class-mapping (class-mapping-of class-node)))
    (dolist (superclass-mapping (superclass-mappings-of class-mapping))
      (select-all-inheritance-class-nodes context root
					  (superclass-node-of
					   (ensure-direct-inheritance context
								      root
								      class-node
								      superclass-mapping))))
    (select-class-node class-node context)))

(defun ensure-subclass-root (superclass-root subclass-mapping query-root) 
  (let ((class-mapping 
	 (get-class-mapping
	  (reference-class-of subclass-mapping))))
    (multiple-value-bind (subclass-root presentp)
	(gethash class-mapping (subclass-roots-of query-root))
      (if (not presentp)
	  (make-instance 'subclass-root ; see initialize-instance
			 :class-mapping class-mapping
			 :foreign-key (foreign-key-of subclass-mapping)
			 :superclass-root superclass-root
			 :query-root query-root)
	  subclass-root))))

(defun select-subclasses (superclass-root query-root) ; query-root for list-subclass-roots
  (dolist (subclass-mapping
	    (subclass-mappings-of
	     (class-mapping-of superclass-root)) superclass-root)
    (select-subclasses (ensure-subclass-root superclass-root
					     subclass-mapping
					     query-root)
		       query-root)))

(defgeneric find-class-node (class-mapping root))

(defmethod find-class-node (class-mapping (root subclass-root))
  (or (get-class-node root class-mapping)
      (find-class-node class-mapping (superclass-root-of root))))

(defmethod find-class-node (class-mapping (root query-root))
  (get-class-node root class-mapping))

(defun select-subclass-inheritance-class-nodes (context root class-node)
  (select-class-node class-node context)
  (dolist (superclass-mapping
	    (superclass-mappings-of (class-mapping-of class-node)))
    (let ((class-mapping
	   (get-class-mapping
	    (reference-class-of superclass-mapping))))
      (when (not (find-class-node class-mapping root))
	(let ((superclass-node
	       (superclass-node-of
		(ensure-direct-inheritance context root class-node
					   superclass-mapping))))
	  (select-subclass-inheritance-class-nodes context root
						   superclass-node))))))
  
(defun select-class (context root)
  (select-all-inheritance-class-nodes context
				      root
				      (ensure-class-node context root
							 (class-mapping-of root)))
  (select-subclasses root root)
  (dolist (subclass-root (list-subclass-roots root))
    (select-subclass-inheritance-class-nodes
     (ensure-class-node context subclass-root
			(class-mapping-of subclass-root)) context root)))

(defun ensure-path-class-nodes (context root class-mapping path)
  (reduce #'(lambda (class-node superclass-mapping)
	      (superclass-node-of
	       (ensure-direct-inheritance context root class-node
					  superclass-mapping)))
	  path
	  :from-end t
	  :initial-value (ensure-class-node context root class-mapping)))

(defgeneric parse-equality (context lhs-expression rhs-expression))

(defmethod parse-equality (context lhs-expression rhs-expression))

(defmethod parse-equality :after (context (lhs-root root) (rhs-root root))
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
		 (column-of (property-mapping-of property-slot))))

(defun ensure-property (context property-slot)
  (ensure-column context (ensure-slot-path context property-slot)
		 (column-of (property-mapping-of property-slot))))

(defun ensure-root-primary-key (context root)
  (let* ((class-mapping
	  (class-mapping-of root))
	 (class-node
	  (ensure-class-node context root class-mapping)))
    (dolist (column-name (primary-key-of class-mapping))
      (ensure-column context class-node column-name))))

(defgeneric ensure-reference (context reference mapped-slot))

(defmethod ensure-reference (context reference mapped-slot))

(defmethod ensure-reference :after (context reference (mapped-slot one-to-many-slot))
  (let ((class-node
	 (ensure-slot-path context mapped-slot))
	(reference-class-node
	 (ensure-class-node context reference (class-mapping-of reference))))
    (dolist (column-name (primary-key-of (class-mapping-of class-node)))
      (ensure-column context class-node column-name))
    (dolist (column-name (foreign-key-of (reference-mapping-of mapped-slot)))
      (ensure-column context reference-class-node column-name))))

(defmethod ensure-reference :after (context reference (mapped-slot many-to-one-slot))
  (let ((class-node
	 (ensure-slot-path context mapped-slot))
	(reference-class-node
	 (ensure-class-node context reference (class-mapping-of reference))))
    (dolist (column-name (foreign-key-of (reference-mapping-of mapped-slot)))
      (ensure-column context class-node column-name))
    (dolist (column-name (primary-key-of (class-mapping-of reference-class-node)))
      (ensure-column context reference-class-node column-name))))

(defgeneric parse-recursive-expression (context expression))

(defmethod parse-recursove-expression (context expression))

(defmethod parse-recursive-expression :after (context (expression property-slot))
  (select-property context expression))

(defmethod parse-recursive-expression :after (context (expression root))
  (ensure-root-primary-key context expression))

(defmethod parse-recursive-expression :after (context (expression joined-reference))
  (ensure-reference context expression (reference-slot-of expression)))

(defgeneric parse-expression (context expression))

(defmethod parse-expression (context expression))

(defmethod parse-expression :after (context (expression n-ary-expression))
  (dolist (expression (arguments-of expression))
    (parse-expression context expression)))

(defmethod parse-expression :after (context (expression binary-operation))
  (parse-expression context (lhs-expression-of expression))
  (parse-expression context (rhs-expression-of expression)))

(defmethod parse-expression :after (context (expression equality))
  (parse-equality context (lhs-expression-of expression)
		  (rhs-expression-of expression)))

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
    (parse-aggregate-expression context argument)))

(defmethod parse-aggregate-expression :after (context (expression binary-expression))
  (parse-aggregate-expression context (lhs-expression-of expression))
  (parse-aggregate-expression context (rhs-expression-of expression)))

(defmethod parse-aggregate-expression :after (context (expression equality))
  (parse-equality context (lhs-expression-of expression)
		  (rhs-expression-of expression)))

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

(defmethod parse-select-item :after (context (expression equality))
  (parse-equality context (lhs-expression-of expression)
		  (rhs-expression-of expression)))

(defmethod parse-select-item :after (context (expression root))
  (ensure-class-node context expression (class-mapping-of expression)))

(defmethod parse-select-item :after (context (expression joined-reference))
  (ensure-slot-path context (reference-slot-of expression)))

(defmethod parse-select-item :after (context (expression property-slot))
  (ensure-property context expression))

(defmethod parse-select-item :after (context (expression aggregate-expression))
  (when (not (group-by-present-p context))
    (setf (group-by-present-p context) t))
  (dolist (expression (arguments-of expression))
    (parse-aggregate-expression context expression)))

(defgeneric parse-select-list (context expression))

(defmethod parse-select-list (context (expression t)))

(defmethod parse-select-list :after (context (expression root))
  (select-class context expression))

(defmethod parse-select-list :after (context (expression joined-reference))
  (ensure-slot-path context (reference-slot-of expression)))

(defmethod parse-select-list :after (context (expression property-slot))
  (select-property context expression))

(defmethod parse-select-list :after (context (expression expression))
  (parse-select-item context expression)
  (setf (gethash expression (expression-aliases-of context))
	(make-alias (name-of expression))))

(defgeneric parse-order-by-clause (context expression))

(defmethod parse-order-by-clause (context expression))

(defmethod parse-order-by-clause :after (context (expression ascending))
  (parse-expression context (expression-of expression)))

(defmethod parse-order-by-clause :after (context (expression descending))
  (parse-expression context (expression-of expression)))

(defgeneric parse-fetch-clause (context fetch-reference))

(defmethod parse-fetch-clause (context fetch-reference))

(defmethod parse-fetch-clause :after (context (fetched-reference fetched-reference))
  (ensure-slot-path context (reference-slot-of fetched-reference))
  (select-class context fetched-reference))

;; parse of order-by-clause and having-clause not needed becouse this
;; expressins compute on select-list
(defmethod initialize-instance ((instance context)
				&key previous-context query aux-clause
				  recursive-clause select-list
				  where-clause order-by-clause
				  having-clause fetch-clause 
				  &allow-other-keys)
  (when (not (null previous-context))
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
  (dolist (expression having-clause)
    (parse-expression instance expression))
  (dolist (expression fetch-clause)
    (parse-fetch-clause instance expression))
  (dolist (expression order-by-clause)
    (parse-order-by-clause instance expression)))

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
		       fetch-clause recursive-clause)
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
					  :fetch fetch-clause
					  :recursive recursive-fetch)
			    :select select-list
			    :fetch fetch-clause
			    :order-by order-by-clause)
	      (make-context query "main"
			    :previous-context selection
			    :select select-list
			    :fetch fetch-clause
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
    (make-selection query (ensure-recursive-joining query aux-clause
						    recursive-clause)
		    select-list where-clause order-by-clause
		    having-clause limit offset fetch-clause
		    recursive-clause)))

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
