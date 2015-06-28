(in-package #:cl-db)

(defvar *table-index*)

;; Query root

(defclass class-node ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (superclass-nodes :reader superclass-nodes-of)
   (alias :initarg :alias
	  :reader table-alias-of) ; возможно его нужно определить в элементе select-list
   (path :initarg :path
	 :reader path-of)))

(defclass superclass-node (class-node)
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass concrete-class-node (class-node)
  ((references :reader references-of)))

(defclass root-node (concrete-class-node)
  ((properties :reader properties-of)))

(defclass reference-node (root-node) ;; for joins
  ((reference :initarg :reference
	      :reader reference-of)
   (parent-node :initarg :parent-node
		:reader parent-node-of)))

(defun make-alias (&rest name-parts)
  (format nil "~{~(~a~)_~}~a" name-parts (incf *table-index*)))	  

(defmethod initialize-instance :after ((instance class-node)
				       &key class-mapping path
					 (superclass-mappings
					  (superclass-mappings-of class-mapping)))
  (with-slots (superclass-nodes)
      instance
    (setf superclass-nodes
	  (mapcar #'(lambda (superclass-mapping)
		      (make-instance 'superclass-node
				     :alias (make-alias)
				     :class-mapping (get-class-mapping
						     (reference-class-of superclass-mapping))
				     :foreign-key (foreign-key-of superclass-mapping)
				     :path (list* instance path)))
		  superclass-mappings))))

(defun plan-superclasses-references (class-node)
  (reduce #'append
	  (superclass-nodes-of class-node)
	  :key #'plan-references
	  :from-end t))

(defun plan-references (class-node)
  (let ((class-mapping
	 (class-mapping-of class-node)))
    (reduce #'(lambda (reference result)
		(acons reference class-node result))
	    (append
	     (many-to-one-mappings-of class-mapping)
	     (one-to-many-mappings-of class-mapping))
	    :from-end t
	    :initial-value (plan-superclasses-references class-node))))

(defmethod initialize-instance :after ((instance concrete-class-node)
				       &key &allow-other-keys)
  (setf (slot-value instance 'references)
	(plan-references instance)))

(defun plan-superclasses-properties (class-node)
  (reduce #'append
	  (superclass-nodes-of class-node)
	  :key #'plan-properties
	  :from-end t))

(defun plan-properties (class-node)
  (reduce #'(lambda (property result)
	      (acons property class-node result))
	  (property-mappings-of
	   (class-mapping-of class-node))
	  :from-end t
	  :initial-value (plan-superclasses-properties class-node)))

(defmethod initialize-instance :after ((instance root-node)
				       &key &allow-other-keys)
  (setf (slot-value instance 'properties)
	(plan-properties instance)))

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

(defun join (concrete-class-node reader &key alias join)
  (destructuring-bind (reference . class-node)
      (assoc (get-slot-name
	      (find-class
	       (class-name-of
		(class-mapping-of concrete-class-node))) reader)
	     (references-of concrete-class-node)
	     :key #'slot-name-of)
    (let* ((reference-node
	    (make-instance 'reference-node
			   :class-mapping (get-class-mapping
					   (reference-class-of reference))
			   :class-node class-node
			   :reference reference))
	   (args
	    (when (not (null join))
	      (multiple-value-call #'append
		(funcall join reference-node)))))
      (if (not (null alias))
	  (list* alias reference-node args)
	  args))))

;; WHERE clause, WITH-statent clauses (WHERE and RECURSIVE WHERE)

;;; Operators, functions, aggregate functions

(defclass expression ()
  ())

(defclass operation (expression)
  ((operator :initarg :operator :reader operator-of)))

(defclass binary-operation (operation)
  ((lhs-expression :initarg :lhs :reader lhs-expression-of)
   (rhs-expression :initarg :rhs :reader rhs-expression-of)))

(defclass binary-operator-extended (operation)
  ((args :initarg :args
	 :reader args-of)))

(defclass sql-function (expression)
  ((arguments :initarg :arguments
	      :reader arguments-of)))

(defclass rsbms-function-call (sql-function) ;; call by name
  ((function-name :initarg :function-name
		  :reader function-name-of)))

(defclass aggregation (sql-function)
  ())

(defclass rdbms-aggregation (aggregation) ;; call by name
  ((function-name :initarg :function-name
		  :reader function-name-of)))

(defclass sort-direction (operation)
  ((expression :initarg :expression
	       :reader expression-of)))

(defclass ascending (sort-direction)
  ())

(defclass descending (sort-direction)
  ())

(defmacro define-binary-operation (name)
  `(defclass ,name (binary-operation)
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
		 :args (list* restriction more-restrictions)))

(defun conjunction (restriction &rest more-restrictions)
  (make-instance 'conjunction
		 :args (list* restriction more-restrictions)))

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
  (make-instance 'conjunction :args 
		 (loop for (class-name . parameter) in operations
		    when (not (null parameter))
		    collect (make-instance class-name
					   :lhs property
					   :rhs parameter)))))

(defgeneric projection (descriptor &rest args))

;;; заменить всю диспечеризацию на методы (eql symbol)
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

(defvar *common-table-expression*)

(defclass recursive-class-node ()
  ((common-table-expression :initarg :common-table-expression
			    :reader common-table-expression-of)
   (class-node :initarg :class-node
	       :reader class-node-of)))

(defmethod references-of ((node recursive-class-node))
  (mapcar #'(lambda (reference)
	      (cons (first reference) node))
	  (references-of
	   (class-node-of node))))

(defmethod class-mapping-of ((class-node recursive-class-node))
  (class-mapping-of (class-node-of class-node)))

;; redefine property search for recursive nodes
(defmethod properties-of ((class-node recursive-class-node))
  (mapcar #'(lambda (property)
	      (cons (car property) class-node))
	  (properties-of (class-node-of class-node))))

(defun recursive (class-node &optional (common-table-expression *common-table-expression*))
  (make-instance 'recursive-class-node :class-node class-node
		 :common-table-expression common-table-expression))

(defun ascending (arg)
  (make-instance 'ascending :arg arg))

(defun descending (arg)
  (make-instance 'descending :arg arg))

;; Select list

;;; Property

(defgeneric property (node reader))

(defmethod property ((root-node root-node) reader)
  (let* ((class-name
	  (class-name-of (class-mapping-of root-node)))
	 (slot-name
	  (get-slot-name (find-class class-name) reader)))
    (or
     (assoc slot-name
	    (properties-of root-node)
	    :key #'slot-name-of)
     (error "Property mapping for slot-name ~a of class mapping ~a not found"
	    slot-name class-name))))

(defmethod property ((recursive-node recursive-class-node) reader)
  (let ((property-node
	 (property (class-node-of recursive-node) reader)))
    (cons (first property-node) recursive-node)))

(defclass expression-selection ()
  ((alias :initarg :alias
	  :reader alias-of)
   (expression :initarg :expression
	       :reader expression-of)))

;;; Class selection

(defclass select-item () ; i.e. for sql expressions, property selection, columns etc.
  ((expressions :initarg :expressions
		:reader expressions-of)
   (from-clause :initarg :from-clause
		:reader from-clause-of)))

(defclass root-node-selection (select-item)
  ((root-node :initarg :root-node
	      :reader root-node-of)))

(defclass class-selection (root-node-selection)
  ((references :reader references-of)
   (subclass-selections :reader subclass-selections-of)))

(defclass reference-fetching (class-select-item)
  ((reference :initarg :reference :reader reference-of)
   (class-selection :initarg :class-selection)))

(defclass cte-select-item (select-item) 
  ((select-item :initarg :select-item ;;proxied select-item
		:reader select-item-of)))

(defun make-subclass-node (subclass-mapping parent-node)
  (let ((class-mapping
	 (get-class-mapping
	  (reference-class-of subclass-mapping))))
    (make-instance 'subclass-node
		   :class-mapping class-mapping
		   :superclass-mappings (remove
					 (class-name-of class-mapping)
					 (superclass-mappings-of class-mapping)
					 :key #'reference-class-of)
		   :foreign-key (foreign-key-of subclass-mapping)
		   :parent-node parent-node)))

(defun plan-fetch-references (class-selection)
  (reduce #'append
	  (subclass-nodes-of class-selection)
	  :key #'plan-subclass-references
	  :from-end t))

(defun plan-subclass-references (class-selection)
  (let ((class-mapping
	 (class-mapping-of class-selection)))
    (reduce #'(lambda (reference result)
		(acons reference class-selection result))
	    (append
	     (many-to-one-mappings-of class-mapping)
	     (one-to-many-mappings-of class-mapping))
	    :from-end t
	    :initial-value (plan-fetch-references class-selection))))

(defmethod initialize-instance :after ((instance class-selection) &key root-node)
  (with-slots (columns subclass-nodes references)
      instance
    (setf expressions ;; columns
	  (reduce #'(lambda (column-name result)
		      (acons column-name
			     (make-alias column-name)
			     result))
		  (columns-of
		   (class-mapping-of root-node))
		  :initial-value nil))
    (setf subclass-nodes
	  (mapcar #'(lambda (subclass-mapping)
		      (make-subclass-node subclass-mapping root-node))
		  (subclass-mappings-of
		   (class-mapping-of root-node))))
    (setf references (append
		      (references-of root-node)
		      (plan-fetch-references instance)))))

(defmethod initialize-instance :after ((instance subclass-node)
				       &key class-mapping)
  (setf (slot-value instance 'subclass-nodes)
	(mapcar #'(lambda (subclass-mapping)
		    (make-subclass-node subclass-mapping instance))
		(subclass-mappings-of class-mapping))))

(defgeneric select-item (expression))

(defmethod select-item ((expression list))
  (make-instance 'expression-selection
		 :expression expression
		 :alias (make-alias
			 (string-downcase
			  (slot-name-of (first expression))))))

(defmethod select-item ((expression expression))
  (make-instance 'expression-selection
		 :expression expression
		 :alias (make-alias
			 (string-downcase
			  (class-name-of expression)))))

(defmethod select-item ((expression root-node))
  (make-instance 'class-selection :root-node expression))

(defmethod select-item ((expression recursive-class-node))
  (make-instance 'class-selection :root-node expression))

(defmethod initialize-instance :after ((instance fetched-reference)
				       &key recursive fetch)
  (with-slots (recursive-clause fetched-references)
      instance
    (setf recursive-clause
	  (when (not (null recursive))
	    (restrict
	     (recursive (root-node-of instance)) :equal recursive)))
    (setf fetched-references
	  (when (not (null fetch))
	    (multiple-value-list
	     (funcall fetch instance))))))

(defun fetch (class-selection reader &key fetch recursive)
  (let ((reference
	 (assoc (get-slot-name
		 (find-class
		  (class-name-of
		   (class-mapping-of (root-node-of class-selection)))) reader)
		(references-of class-selection)
		:key #'slot-name-of)))
    (make-instance 'fetched-reference
		   :root-node (make-instance 'concrete-class-node
					     :class-mapping (get-class-mapping
							     (reference-class-of
							      (first reference))))
		   :class-selection class-selection
		   :reference reference
		   :recursive recursive
		   :fetch fetch)))

(defclass query ()
  ((select-list :initarg :select-list
		:accessor select-list-of)
   (from-clause :initarg :from-clause
		:reader from-clause-of)
   (where-clause :initarg :where-clause
		 :initform nil
		 :reader where-clause-of)
   (having-clause :initarg :having-clause
		  :reader having-clause-of)
   (order-by-clause :initarg :order-by-clause
		    :accessor order-by-clause-of)
   (limit :initarg :limit
	  :accessor limit-of)
   (offset :initarg :offset
	   :accessor offset-of)))

(defclass common-table-expression ()
  ((name :initarg :name
	 :reader name-of)
   (query :initarg :query
	  :reader query-of)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)))

(defclass sql-expression (query)
  ((common-table-expressions :initform (list)
			     :initarg :common-table-expressions
			     :reader common-table-expressions-of)))

(defun make-root (class-name)
  (make-instance 'root-node :class-mapping (get-class-mapping class-name)))

(defun compute-joined-list (selectors join &key aux &allow-other-keys)
  (let ((from-clause
	 (multiple-value-call #'append selectors
			      (when (not (null join))
				(apply join selectors)))))
    (make-instance 'sql-expression
		   :from-clause from-clause
		   :where-clause (when (not (null aux))
			    (multiple-value-list
			     (apply aux from-clause))))))

(defun make-joined-list (joined-list cte)
  (reduce #'(lambda (joined-list class-node)
	      (substitute
	       (recursive class-node cte) class-node joined-list))
	  (remove-if #'keywordp joined-list)
	  :initial-value joined-list))

(defmethod initialize-instance :after ((instance common-table-expression)
				       &key recursive query)
  (let ((*common-table-expression* instance))
    (with-slots (recursive-clause)
	instance
      (setf recursive-clause
	    (when (not (null recursive))
	      (multiple-value-list
		(apply recursive (from-clause-of query))))))))

(defun wrap-query (sql-expression name &optional recursive-clause)
  (let ((cte
	 (make-instance 'common-table-expression
			:name name
			:query sql-expression
			:recursive recursive-clause)))
    (make-instance 'sql-expression :common-table-expressions
		   (list* cte (common-table-expressions-of sql-expression))
		   :from-clause (make-joined-list
				 (from-clause-of sql-expression) cte))))

(defun ensure-aux-clause (sql-expression &key aux &allow-other-keys)
  (when (not (null aux))
    (with-slots (where-clause)
	sql-expression
      (setf where-clause
	    (append where-clause
		    (multiple-value-list 
		     (apply aux (from-clause-of sql-expression)))))))
  sql-expression)

(defun ensure-recursive-join (sql-expression &key recursive &allow-other-keys)
  (if (not (null recursive))
      (wrap-query sql-expression "recursive_join" recursive)
      sql-expression))

(defun ensure-where-clause (sql-expression &key where &allow-other-keys)
  (when (not (null where))
    (with-slots (where-clause)
	sql-expression
      (setf where-clause
	    (append where-clause
		    (multiple-value-list 
		     (apply where (from-clause-of sql-expression)))))))
  sql-expression)

(defun ensure-having-clause (sql-expression &key having &allow-other-keys)
  (with-slots (having-clause)
      sql-expression
    (setf having-clause
	  (when (not (null having))
	    (multiple-value-list
	     (apply having (from-clause-of sql-expression))))))
  sql-expression)

(defun ensure-limit (sql-expression &key limit &allow-other-keys)
  (when (not (null limit))
    (setf (limit-of sql-expression) limit))
  sql-expression)

(defun ensure-offset (sql-expression &key offset &allow-other-keys)
  (when (not (null offset))
    (setf (offset-of sql-expression) offset))
  sql-expression)

(defun ensure-selection (sql-expression &key select &allow-other-keys)
  (let ((joined-list
	 (from-clause-of sql-expression)))
    (setf (select-list-of sql-expression)
	  (mapcar #'select-item
		  (if (not (null select))
		      (multiple-value-list
		       (apply select joined-list))
		      (subseq joined-list 0 (position-if #'keywordp joined-list)))))) ; roots
  sql-expression)

(defun make-recursive-clause (fetch-references)
  (reduce #'append fetch-references
	  :key #'(lambda (fetch-reference)
		   (let ((clause
			  (make-recursive-clause
			   (fetched-references-of fetch-reference)))
			 (recursive-clause
			  (recursive-clause-of fetch-reference)))
		     (if (not (null recursive-clause))
			 (list* recursive-clause clause)
			 clause)))
	  :from-end t
	  :initial-value nil))

(defun ensure-recursive-fetch (sql-expression fetch-clause)
  (let ((recursive-clause
	 (make-recursive-clause fetch-clause)))
    (if (not (null recursive-clause))
	(wrap-query sql-expression "recursive_fetching" recursive-clause)
	sql-expression)))

(defun ensure-fetch (sql-expression &key fetch limit offset &allow-other-keys)
  (if (not (null fetch))
      (let ((fetch-clause
	     (multiple-value-list
	      (apply fetch (select-list-of sql-expression)))))
	(dolist (fetched-reference fetch-clause)
	  (push fetched-reference
		(fetched-references-of
		 (class-selection-of fetched-reference))))
	(ensure-recursive-fetch
	 (if (not (null (or limit offset)))
	     (wrap-query sql-expression "before_fetch")
	     sql-expression)
	 fetch-clause))
      sql-expression))

(defun ensure-order-by (sql-expression &key order-by &allow-other-keys)
  (when (not (null order-by))
    (setf (order-by-clause-of sql-expression)
	  (apply order-by (from-clause-of sql-expression))))
  sql-expression)

(defun make-query (roots join &rest args)
  (let* ((*table-index* 0)
	 (selectors
	  (if (listp roots)
	      (mapcar #'(lambda (root)
			  (make-root root))
		      roots)
	      (list (make-root roots)))))
    (reduce #'(lambda (sql-expression function)
		(apply function sql-expression args))
	    (list #'ensure-aux-clause
		  #'ensure-recursive-join
		  #'ensure-where-clause
		  #'ensure-having-clause
		  #'ensure-limit
		  #'ensure-offset
		  #'ensure-selection
		  #'ensure-fetch
		  #'ensure-order-by)
	    :initial-value (compute-joined-list selectors join))))

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

(defun append-children (appended-joins join-fn table-name alias on-clause &rest joins)
  (list* join-fn table-name alias on-clause
	 (reduce #'(lambda (result appended-join)
		     (apply #'join-append result appended-join))
		 appended-joins :initial-value joins)))

(defun join-append (joins join-fn table-name alias on-clause &rest appended-joins)
  (let ((join (find alias joins :key #'third)))
    (if (not (null join))
	(list*
	 (apply #'append-children appended-joins join)
	 (remove alias joins :key #'third))
	(list* (list* join-fn table-name alias on-clause appended-joins)
	       joins))))

(defun root-append (appended-joins table-reference-fn table-name alias &rest joins)
  (list* table-reference-fn table-name alias
	 (reduce #'(lambda (result join)
		     (apply #'join-append result join))
		 appended-joins :initial-value joins)))

(defun from-clause-append (from-clause table-reference-fn table-name alias &rest joins)
  (let ((root
	 (or
	  (find alias from-clause :key #'third)
	  (find #'write-subquery from-clause :key #'first))))
    (if (not (null root))
	(list*
	 (apply #'root-append joins root)
	 (remove (third root) from-clause :key #'third))
	(list* (list* table-reference-fn table-name alias joins)
	       from-clause))))
