(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&rest name-parts)
  (format nil "~{~(~a~)_~}~a" name-parts (incf *table-index*)))

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

;;;;

(defclass root ()
  ((class-nodes :initform (make-hash-table)
		:reader class-nodes-of)
   (superclass-paths :reader superclass-paths-of)
   (reference-slots :reader reference-slots-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (direct-subclass-roots :initform (list)
			  :accessor direct-subclasses-of)))

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
			  (compute-superclass-paths
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

(defclass class-root (root)
  ((subclass-roots :initform (make-hash-table)
		   :reader subclass-roots-of)))

(defmethod initialize-instance :after ((instance class-root) &key &allow-other-keys)
  (select-subclasses instance instance))

(defun ensure-subclass-root (subclass-mapping query-root) 
  (let ((class-mapping
	 (get-class-mapping
	  (reference-class-of subclass-mapping))))
    (ensure-gethash class-mapping
		    (subclass-roots-of query-root)
		    (make-instance 'subclass-root ; see initialize-instance
				   :class-mapping class-mapping
				   :foreign-key (foreign-key-of subclass-mapping)
				   :query-root query-root))))

(defun add-subclass-root (subclass-root superclass-root)
  (push superclass-root (superclass-roots-of subclass-root))
  (push subclass-root (direct-subclasses-of superclass-root)))

(defun select-subclasses (superclass-root query-root) ; query-root for list-subclass-roots
  (dolist (subclass-mapping
	    (subclass-mappings-of
	     (class-mapping-of superclass-root)) superclass-root)
    (let ((subclass-root
	   (ensure-subclass-root subclass-mapping query-root)))
      (add-subclass-root subclass-root superclass-root)
      (select-subclasses subclass-root query-root))))

(defclass query-root (class-root)
  ((property-slots :reader property-slots-of)))

(defun get-subclass-root (class-mapping root)
  (multiple-value-bind (subclass-root presentp)
      (gethash class-mapping (subclass-roots-of root))
    (when (not presentp)
      (error "Subclass ~a for ~a not found" (class-name-of class-mapping)
	     (class-name-of (class-mapping-of root))))
    subclass-root))

(defun list-subclass-roots (query-root)
  (hash-table-values (subclass-roots-of query-root)))

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

(defun property (query-root reader)
  (let* ((class-name
	  (class-name-of (class-mapping-of query-root)))
	 (slot-name
	  (get-slot-name (find-class class-name) reader)))
    (or
     (find slot-name (property-slots-of query-root)
	   :key (compose #'slot-name-of #'property-mapping-of))
     (error "Property mapping for slot-name ~a of class mapping ~a not found"
	    slot-name class-name))))

(defun compute-properties (root class-mapping)
  (iterate-inheritance #'(lambda (result class-mapping path)
			   (reduce #'list* (property-mappings-of class-mapping)
				   :from-end t
				   :key #'(lambda (property-mapping)
					    (make-instance 'property-slot
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
   (superclass-roots :initform (list)
		     :accessor superclass-roots-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defmethod initialize-instance :after ((instance subclass-root)
				       &key class-mapping query-root)
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

(defclass fetched-reference (class-root)
  ((reference-slot :initarg :reference-slot
		      :reader reference-slot-of)
   (superclass-root :initarg :superclass-root
		    :reader superclass-root-of)
   (recursive-node :initarg :recursive-node
		   :reader recursive-node-of)))

(defun get-reference-slot (root reader)
  (let* ((class-mapping
	  (class-mapping-of root))
	 (class-name
	  (class-name-of class-mapping))
	 (slot-name
	  (get-slot-name
	   (find-class class-name) reader)))
    (or (find slot-name (reference-slots-of root)
	      :key (compose #'slot-name-of #'reference-mapping-of))
	(error "Reference ~a of class ~a not found"
	       slot-name class-name))))

(defun fetch (root reader &key subclass-name fetch recursive)
  (let* ((class-root
	  (if (not (null subclass-name))
	      (get-subclass-root
	       (get-class-mapping subclass-name) root)
	      root))
	 (reference-slot
	  (get-reference-slot class-root reader))
	 (fetched-reference
	  (make-instance 'fetched-reference
			 :class-mapping (get-class-mapping
					 (reference-class-of
					  (reference-mapping-of reference-slot)))
			 :reference-slot reference-slot
			 :recursive-node recursive
			 :root class-root)))
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
     (or (assoc column-name columns :test #'string=)
	 (assoc column-name
		(setf (gethash class-node (class-node-columns-of context))
		      (acons column-name
			     (if (not (eq context (context-of class-node)))
				 (ensure-external-column-selection
				  (previous-context-of context) class-node column-name)
				 (make-instance 'column
						:correlation class-node
						:name column-name))
			     columns))
		:test #'string=)))))

(defclass class-node ()
  ((root :initarg :root
	 :reader root-of)
   (context :initarg :context
	    :reader context-of)
   (alias :initarg :alias
	  :reader alias-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (superclass-nodes :initform (make-hash-table)
		     :reader superclass-nodes-of)
   (reference-mappings :initform (make-hash-table)
		       :reader reference-mappings-of)))

(defun get-superclass-node (class-mapping class-node)
  (gethash class-mapping (superclass-nodes-of class-node)))

(defun get-subclass-node (class-mapping class-node)
  (gethash class-mapping (subclass-node-of class-node)))

(defun get-root-class-nodes (root context)
  (gethash root (class-nodes-of context)))

(defgeneric get-query-root (root))

(defmethod get-query-root ((query-root query-root))
  query-root)

(defmethod get-query-root ((subclass-root subclass-root))
  (get-query-root (query-root-of subclass-root)))

(defmethod get-query-root ((fetched-reference fetched-reference))
  (get-query-root (root-of (reference-slot-of fetched-reference))))

(defmethod get-query-root ((joined-reference joined-reference))
  (get-query-root (root-of (reference-slot-of joined-reference))))

(defmethod initialize-instance :after ((instance class-node)
				       &key context root class-mapping
					 &allow-other-keys)
  (multiple-value-bind (class-node presentp)
      (gethash class-mapping (class-nodes-of root))
    (if (not presentp)
	(setf (gethash class-mapping (class-nodes-of root)) instance)
	(error "Class node for class mapping ~a for root ~a already present"
	       class-node root)))
  (push instance (gethash (get-query-root root)
			  (class-nodes-of context)))
  (setf (slot-value instance 'alias)
	(make-alias (table-name-of class-mapping))))

(defclass superclass-node (class-node)
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)
   (subclass-node :initarg :subclass-node
		  :reader subclass-node-of)))

(defmethod initialize-instance :after ((instance superclass-node)
				       &key subclass-node foreign-key
					 context class-mapping
					 &allow-other-keys)
  (setf (slot-value instance 'foreign-key)
	(mapcar #'(lambda (pk-column fk-column)
		    (list 
		     (ensure-column context instance pk-column)
		     (ensure-column context subclass-node fk-column)))
		(primary-key-of class-mapping)
		foreign-key)))

(defclass root-node (class-node)
  ((subclass-nodes :initform (make-hash-table)
		   :reader subclass-nodes-of)))

(defclass subclass-node (root-node)
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)
   (superclass-node :initarg :superclass-node
		    :reader supeclass-node-of)))

(defmethod initialize-instance :after ((instance subclass-node)
				       &key context class-mapping
					 superclass-node foreign-key
					 &allow-other-keys)
  (setf (gethash class-mapping (superclass-nodes-of instance))
	superclass-node)
  (setf (slot-value instance 'foreign-key)
	(mapcar #'(lambda (fk-column pk-column)
		    (list 
		     (ensure-column context superclass-node pk-column)
		     (ensure-column context instance fk-column)))
		foreign-key
		(primary-key-of class-mapping))))

(defclass reference-node (root-node)
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)
   (class-node :initarg :class-node
	       :reader clas-node-of)))

(defclass many-to-one-node (reference-node)
  ())

(defmethod initialize-instance :after ((instance many-to-one-node)
				       &key class-node foreign-key
					 context class-mapping
					 &allow-other-keys)
  (setf (slot-value instance 'foreign-key) 
	(mapcar #'(lambda (fk-column pk-column)
		    (list 
		     (ensure-column context class-node fk-column)
		     (ensure-column context instance pk-column)))
		foreign-key
		(primary-key-of class-mapping))))

(defclass one-to-many-node (reference-node)
  ())

(defmethod initialize-instance :after ((instance one-to-many-node)
				       &key class-node foreign-key
					 context class-mapping
					 &allow-other-keys)
  (setf (slot-value instance 'foreign-key)
	(mapcar #'(lambda (pk-column fk-column)
		    (list 
		     (ensure-column context instance pk-column)
		     (ensure-column context class-node fk-column)))
		(primary-key-of class-mapping)
		foreign-key)))

(defclass context ()
  ((name :initarg :name
	 :reader alias-of)
   (previous-context :initarg :previous-context
		     :reader previous-context-of)
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
   (fetch-clause :initarg :fetch-clause
		 :reader fetch-clause-of)
   (limit-clause :initarg :limit-clause
		 :reader limit-clause-of)
   (offset-clause :initarg :offset-clause
		  :reader offset-clause-of)))

(defun get-node-column (context class-node column-name)
  (rest (assoc column-name (gethash class-node (class-node-columns-of context)))))

(defun ensure-superclass-node (context subclass-node superclass-mapping)
  (let ((class-mapping
	 (get-class-mapping
	  (reference-class-of superclass-mapping)))
	(root (root-of subclass-node)))
    (or (find-class-node class-mapping root)
	(ensure-gethash class-mapping (superclass-nodes-of subclass-node)
			(make-instance 'superclass-node
				       :foreign-key (foreign-key-of superclass-mapping)
				       :subclass-node subclass-node
				       :class-mapping class-mapping
				       :context context
				       :root root)))))

(defun ensure-subclass-node (context subclass-root superclass-node class-mapping)
  (or (find-class-node class-mapping subclass-root)
      (ensure-gethash class-mapping (subclass-nodes-of superclass-node)
		      (make-instance 'subclass-node
				     :root subclass-root
				     :foreign-key (foreign-key-of subclass-root)
				     :superclass-node superclass-node
				     :class-mapping class-mapping
				     :context context))))

(defgeneric ensure-root-node (context root))

(defun ensure-path-node (context root path)
  (reduce #'(lambda (class-node superclass-mapping)
	      (ensure-superclass-node context class-node superclass-mapping))
	  path
	  :from-end t
	  :initial-value (ensure-root-node context root)))

(defmethod ensure-root-node (context (root root))
  (let ((class-mapping (class-mapping-of root)))
    (or (gethash (class-mapping-of root) (class-nodes-of root))
	(make-instance 'root-node
		       :class-mapping class-mapping
		       :context context
		       :root root))))

(defmethod ensure-root-node (context (joined-reference joined-reference))
  (ensure-reference-node context joined-reference
			 (reference-slot-of joined-reference)))

(defmethod ensure-root-node (context (joined-reference fetched-reference))
  (ensure-reference-node context joined-reference
			 (reference-slot-of joined-reference)))

(defmethod ensure-reference-node (context reference-root (mapped-slot many-to-one-slot))
  (let ((slot-mapping
	 (reference-mapping-of mapped-slot))
	(class-node
	 (ensure-path-node context (root-of mapped-slot) (path-of mapped-slot))))
    (ensure-gethash reference-root (reference-mappings-of class-node)
		    (make-instance 'many-to-one-node
				   :root reference-root
				   :class-mapping (get-class-mapping
						   (reference-class-of slot-mapping))
				   :foreign-key (foreign-key-of slot-mapping)
				   :class-node class-node
				   :context context))))

(defmethod ensure-reference-node (context reference-root (mapped-slot one-to-many-slot))
  (let ((slot-mapping
	 (reference-mapping-of mapped-slot))
	(class-node
	 (ensure-path-node context (root-of mapped-slot) (path-of mapped-slot))))
    (ensure-gethash reference-root (reference-mappings-of class-node)
		    (make-instance 'one-to-many-node
				   :root reference-root
				   :class-mapping (get-class-mapping
						   (reference-class-of slot-mapping))
				   :foreign-key (foreign-key-of slot-mapping)
				   :class-node class-node
				   :context context))))

(defun ensure-alias (expression context)
  (ensure-gethash expression (expression-aliases-of context)
		  (make-alias (name-of expression))))

(defun select-column (context class-node column-name)
  (ensure-alias
   (ensure-column context class-node column-name) context))

(defun select-class-node-columns (class-node context)
  (dolist (column-name (columns-of (class-mapping-of class-node)))
    (select-column context class-node column-name)))

(defun select-all-superclass-nodes (context root class-node)
  (select-class-node-columns class-node context)
  (dolist (superclass-mapping (superclass-mappings-of
			       (class-mapping-of class-node)))
    (let ((superclass-node
	   (ensure-superclass-node context class-node superclass-mapping)))
      (select-all-superclass-nodes context root superclass-node))))

(defgeneric find-class-node (class-mapping root))

(defmethod find-class-node (class-mapping (root subclass-root))
  (or (get-class-node root class-mapping)
      (loop for superclass-root in (superclass-roots-of root)
	 thereis (find-class-node class-mapping superclass-root))))

(defmethod find-class-node (class-mapping (root root))
  (get-class-node root class-mapping))

(defun select-all-subclass-nodes (context root class-node)
  (select-all-superclass-nodes context root class-node)
  (dolist (subclass-root (direct-subclasses-of root))
    (let ((subclass-node
	   (ensure-subclass-node context subclass-root class-node
				 (class-mapping-of subclass-root))))
      (select-all-subclass-nodes context subclass-root subclass-node))))
  
(defun select-class (context root)
  (let ((class-node
	 (ensure-root-node context root)))
    (select-all-subclass-nodes context root class-node)))

(defgeneric parse-equality (context lhs-expression rhs-expression))

(defmethod parse-equality (context lhs-expression rhs-expression))

(defmethod parse-equality :after (context (lhs-root query-root) (rhs-root query-root))
  (ensure-path-node context rhs-root (get-superclass-path rhs-root lhs-root))
  (ensure-path-node context lhs-root (get-superclass-path lhs-root rhs-root)))

(defun select-property (context property-slot)
  (select-column context (ensure-path-node context
					   (root-of property-slot)
					   (path-of property-slot))
		 (column-of (property-mapping-of property-slot))))

(defun ensure-property (context property-slot)
  (ensure-column context (ensure-path-node context
					   (root-of property-slot)
					   (path-of property-slot))
		 (column-of (property-mapping-of property-slot))))

(defun ensure-root-primary-key (context root)
  (let* ((class-mapping
	  (class-mapping-of root))
	 (class-node
	  (ensure-root-node context root)))
    (dolist (column-name (primary-key-of class-mapping))
      (ensure-column context class-node column-name))))

;;(defgeneric ensure-reference (context reference mapped-slot))

;;(defmethod ensure-reference (context reference mapped-slot))

;;(defmethod ensure-reference :after (context reference (mapped-slot one-to-many-slot))
;;  (ensure-root-node context reference mapped-slot))

;;(defmethod ensure-reference :after (context reference (mapped-slot many-to-one-slot))
;;  (ensure-reference-node context reference mapped-slot))

(defgeneric parse-recursive-expression (context expression))

(defmethod parse-recursove-expression (context expression))

(defmethod parse-recursive-expression :after (context (expression property-slot))
  (select-property context expression))

(defmethod parse-recursive-expression :after (context (expression query-root))
  (ensure-root-primary-key context expression))

;;(defmethod parse-recursive-expression :after (context (expression joined-reference))
;;  (ensure-root-node context expression))

(defgeneric parse-expression (context expression))

(defmethod parse-expression (context expression))

(defmethod parse-expression :after (context (expression n-ary-expression))
  (dolist (expression (arguments-of expression))
    (parse-expression context expression)))

(defmethod parse-expression :after (context (expression binary-expression))
  (parse-expression context (lhs-expression-of expression))
  (parse-expression context (rhs-expression-of expression)))

(defmethod parse-expression :after (context (expression equality))
  (parse-equality context (lhs-expression-of expression)
		  (rhs-expression-of expression)))

(defmethod parse-expression :after (context (expression query-root))
  (ensure-root-node context expression))

;;(defmethod parse-expression :after (context (expression joined-reference))
;;  (ensure-path-node context (reference-slot-of expression)))

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

(defmethod parse-aggregate-expression :after (context (expression query-root))
  (ensure-root-node context expression))

;;(defmethod parse-aggregate-expression :after (context (expression joined-reference))
;;  (ensure-slot-path context (reference-slot-of expression)))

(defmethod parse-aggregate-expression :after (context (expression property-slot))
  (ensure-property context expression))

(defgeneric parse-select-item (context expression))

(defmethod parse-select-item (context expression))

(defmethod parse-select-item :after (context (expression n-ary-expression))
  (dolist (expression (arguments-of expression))
    (parse-select-item context expression)))

(defmethod parse-select-item :after (context (expression binary-expression))
  (parse-select-item context (lhs-expression-of expression))
  (parse-select-item context (rhs-expression-of expression)))

(defmethod parse-select-item :after (context (expression equality))
  (parse-equality context (lhs-expression-of expression)
		  (rhs-expression-of expression)))

(defmethod parse-select-item :after (context (expression query-root))
  (ensure-root-node context expression))

;;(defmethod parse-select-item :after (context (expression joined-reference))
;;  (ensure-slot-path context (reference-slot-of expression)))

(defmethod parse-select-item :after (context (expression property-slot))
  (ensure-property context expression))

(defmethod parse-select-item :after (context (expression aggregate-expression))
  (when (not (group-by-present-p context))
    (setf (group-by-present-p context) t))
  (dolist (expression (arguments-of expression))
    (parse-aggregate-expression context expression)))

(defgeneric parse-select-list (context expression))

(defmethod parse-select-list (context (expression t))
  (values expression context))

(defmethod parse-select-list :after (context (expression query-root))
  (select-subclasses expression expression)
  (select-class context expression))

;;(defmethod parse-select-list :after (context (expression joined-reference))
;;  (ensure-slot-path context (reference-slot-of expression)))

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
  (select-class context fetched-reference))

(defmethod initialize-instance :after ((instance context)
				&key aux-clause recursive-clause
				  select-list where-clause
				  order-by-clause having-clause
				  fetch-clause &allow-other-keys)
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
