(in-package #:cl-db)

;; Query root

(defclass class-node ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (superclass-nodes :reader superclass-nodes-of)
   (columns :reader columns-of)
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

(defclass reference-node (root-node)
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
    (setf columns
	  (reduce #'(lambda (column-name result)
		      (acons column-name
			     (make-alias column-name)
			     result))
		  (columns-of class-mapping)))
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

;;; Joins

(defmethod initialize-instance :after ((instance reference-node)
				       &key recursive &allow-other-keys)
  (let ((aux-query-part
	 (make-instance 'aux-query-part :class-node instance)))
    (setf (slot-value instance 'aux-query-part) aux-query-part)
    (setf (slot-value instance 'recursive-condoition)
	  (when (not (null recursive))
	    (funcall recursive aux-query-part)))))

(defun join (concrete-class-node reader &key alias join recursive)
  (let* ((reference
	  (assoc (get-slot-name
		  (find-class
		   (class-name-of
		    (class-mapping-of class-node))) reader)
		 (references-of concrete-class-node)
		 :key #'slot-name-of))
	 (reference-node
	  (make-instance 'reference-node
			 :class-node (first reference)
			 :reference (rest reference)
			 :recursive recursive))
	 (args
	  (when (not (null join))
	    (multiple-value-call #'append
	      (funcall join reference-node)))))
    (if (not (null alias))
	(acons alias reference-node args)
	args)))

;; recursive clause collection. Нужно рекурсивно обойти весь arg-list
;; по зависимостям, до узлов с alias. Каждый reference-node хранит
;; ссылку на предыдущий

;; рекурсивгые части знают

;; Select list

;;; Property

(defun property (root-node reader)
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

(defgeneric select-item (expression))

(defclass property-selection ()
  ((property-mapping :initarg :property-mapping
		     :reader property-mapping-of)
   (class-node :initarg :class-node
	       :reader class-node-of)))

(defmethod select-item ((expression list))
  (destructuring-bind (property-mapping . class-node)
      expression
    (make-instance 'property-selection
		   :class-node class-node
		   :property-mapping property-mapping)))

;;; Class selection

(defclass class-selection ()
  ((root-node :initarg :root-node
	      :reader root-node-of)
   (subclass-nodes :reader subclass-nodes-of)))

(defclass subclass-node (class-node)
  ((parent-node :initarg :parent-node
		:reader parent-node-of)))

(defun make-subclass-node (subclass-mapping parent-node)
  (let ((class-mapping
	 (get-class-mapping
	  (class-name-of subclass-mapping))))
    (make-instance 'subclass-node
		   :class-mapping class-mapping
		   :superclass-mappings (remove
					 (class-name-of class-mapping)
					 (superclass-mappings-of class-mapping))
		   :foreign-key (foreign-key-of subclass-mapping)
		   :parent-node parent-node)))

(defmethod initialize-instance ((instance class-selection) &key root-node)
  (setf (slot-value instance 'subclass-nodes)
	(mapcar #'(lambda (subclass-mapping)
		    (make-subclass-node subclass-mapping root-node))
		(subclass-mappings-of
		 (class-mapping-of root-node)))))

(defmethod initialize-instance :after ((instance subclass-node)
				       &key class-mapping)
  (setf (slot-value instance 'subclass-nodes)
	(mapcar #'(lambda (subclass-mapping)
		    (make-subclass-node subclass-mapping instance))
		(subclass-mappings-of class-mapping))))

(defmethod select-item ((expression root-node))
  (make-instance 'class-selection :root-node expression))

;; WHERE clause

;;; Operators, functions, aggregate functions

(defclass expression ()
  ())

(defclass operation (expression)
  ((operator :initarg :operator :reader operator-of)))

(defclass binary-operation (operation)
  ((lhs-expression :initarg :lhs-expression
		   :reader lhs-expression-of)
   (rhs-expression :initarg :rhs-expression
		   :reader rhs-expression-of)))

(defmacro define-binary-operation (name)
  `(defclass ,name (binary-operation)
     ()))

(define-binary-operation less-than)

(define-binary-operation greater-than)

(define-binary-operation less-than-or-equal)

(define-binary-operation greater-than-or-equal)

(define-binary-operation equal)

(define-binary-operation not-equal)

(define-binary-operation like)

(defclass binary-operator-extended (binary-operation)
  ((rest-expressions :initarg :rest-expressions
		     :reader rest-expressions-of)))

(defmacro define-binary-operator-extended (name)
  `(defclass ,name (binary-operation)
     ()))

(define-binary-operator-extended and-operation)

(define-binary-operator-extended or-operation)

(define-binary-operator-extended addition)

(define-binary-operator-extended subtraction)

(define-binary-operator-extended multiplication)

(define-binary-operator-extended division)

(defclass postfix-operation (operation)
  ((expression :initarg :expression
	       :reader expression-of)))

(defmacro define-postfix-operation (name)
  `(defclass ,name (postfix-operation)
     ()))

(define-postfix-operation is-null)

(define-postfix-operation is-not-null)

(define-postfix-operation is-true)

(define-postfix-operation is-not-true)

(define-postfix-operation ascending)

(define-postfix-operation descending)

(defclass sql-function-call (expression)
  ((function-name :initarg :function-name
		  :reader function-name-of)
   (arguments :initarg :arguments
	      :reader arguments-of)))

(defmacro define-sql-function (name)
  `(defclass ,name (sql-function-call)
     ()))

(define-sql-function abs)

(define-sql-function exp)

(define-sql-function floor)

(define-sql-function log)

(define-sql-function mod)

(define-sql-function power)

(define-sql-function round)

(define-sql-function sqrt)

(define-sql-function trunc)

(define-sql-function acos)

(define-sql-function asin)

(define-sql-function atan)

(define-sql-function cos)

(define-sql-function sin)

(define-sql-function tan)

(defgeneric parse-expression (element &optional result))

(defmethod parse-expression ((element list) &optional result)
  (append
   (list* :begin (reduce #'parse-element element
			 :from-end t :initial-value result))
   (list* :end result)))

(defmethod parse-expression ((element t) &optional result)
  (list* element result))

(defun expression-lexer (expression)
  (let ((parsed-expression
	 (parse-expression expression)))
  #'(lambda ()
      (let ((value (pop parsed-expression)))
	(when (not (null value))
	  (values
	   (cond
	     ((keywordp value) value)
	     ((symbolp value) :symbol)
	     (t :parameter))
	   value))))))

(yacc:define-parser *expression-parser*
  (:start-symbol form)
  (:terminals (:begin :end :symbol :parameter :property
		      :and :or :not :null := :eq :eql :equal :equalp
		      :< :> :<= :>= :<> :!= :+ :- :* :/
		      :abs :sqrt :exp :floor :log
		      :mod :pi :expt :round :truncate
		      :acos :asin :atan :atan2 :cos :sin :tan
		      :like
		      :ascending :descending))
  
  (form
   (:begin expression :end #'(lambda (begin expression end)
			       (declare (ignore begin end))
			       expression))
   (:begin postfix-operation :end #'(lambda (begin expression end)
				      (declare (ignore begin end))
				      expression))
   :parameter)
  
  (postfix-operation
   (:null form #'(lambda (null form)
		   (declare (ignore null))
		   `(make-instance 'is-null :expression ,form)))
   (:not :begin :null form :end #'(lambda (not begin null form end)
				    (declare (ignore not begin null end))
				    `(make-instance 'is-not-null :expression ,form))))
  
  (expression binary-operation binary-operation-extended
	      function-call property sort)

  (sort
   (direction form #'(lambda (direction expression)
		       `(make-instance (quote ,direction)
				       :expression (quote ,expression)))))

  (direction
   (:ascending #'(lambda (op)
		   (declare (ignore op))
		   'ascending))
   (:descending #'(lambda (op)
		    (declare (ignore op))
		    'descending)))

  (property
   (:symbol :symbol #'(lambda (reader class-mapping)
			`(property ,class-mapping
				   (function ,reader)))))
  
  (binary-operation comparison pattern-matching)

  (pattern-matching
   (:like object :parameter #'(lambda (like object pattern)
				(declare (ignore like))
				`(make-instance 'like
						:lhs-expression ,object
						:rhs-expression ,pattern))))

  (comparison
   (comparison-operator form form #'(lambda (op lhs rhs)
				      `(make-instance (quote ,op)
						      :lhs-expression ,lhs
						      :rhs-expression ,rhs))))

  (comparison-operator
   (equal #'(lambda (op)
	      (declare (ignore op))
	      'equal))
   (:< #'(lambda (op)
	   (declare (ignore op))
	   'less-than))
   (:> #'(lambda (op)
	   (declare (ignore op))
	   'more-than))
   (:<= #'(lambda (op)
	   (declare (ignore op))
	   'less-than-or-equal))
   (:>= #'(lambda (op)
	    (declare (ignore op))
	    'more-than-or-equal))
   (:<> #'(lambda (op)
	    (declare (ignore op))
	    'not-equal))
   (:!= #'(lambda (op)
	    (declare (ignore op))
	    'not-equal)))

  (equal := :eq :eql :equal :equalp)

  (binary-extended-operator
   (:and #'(lambda (op)
	    (declare (ignore op))
	    'and-operation))
   (:or #'(lambda (op)
	    (declare (ignore op))
	    'or-operation))
   (:+ #'(lambda (op)
	    (declare (ignore op))
	    'addition))
   (:- #'(lambda (op)
	    (declare (ignore op))
	    'subtraction))
   (:* #'(lambda (op)
	    (declare (ignore op))
	    'multiplication))
   (:/ #'(lambda (op)
	    (declare (ignore op))
	    'division)))

  (binary-operation-extended
   (binary-extended-operator forms #'(lambda (op forms)
				       `(make-instance (quote ,op)
						       :lhs-expression (first (quote ,forms))
						       :reast-expressions (rest (quote forms))))))
  
  (forms
   (form forms)
   (form))

  (sql-function-call function-call aggregation)
  
  (function-call
   (sql-function arguments #'(lambda (sql-function arguments)
			       `(make-instance (quote ,sql-function)
					       :argumets (quote ,arguments)))))
  
  (sql-function
   (:abs #'(lambda (op)
	    (declare (ignore op))
	    'abs))
   (:sqrt #'(lambda (op)
	      (declare (ignore op))
	      'sqrt))
   (:exp #'(lambda (op)
	    (declare (ignore op))
	    'exp))
   (:floor #'(lambda (op)
	       (declare (ignore op))
	       'floor))
   (:log #'(lambda (op)
	    (declare (ignore op))
	    'log))
   (:mod #'(lambda (op)
	    (declare (ignore op))
	    'mod))
   (:pi #'(lambda (op)
	    (declare (ignore op))
	    'pi))
   (:expt #'(lambda (op)
	      (declare (ignore op))
	      'power))
   (:round #'(lambda (op)
	       (declare (ignore op))
	       'round))
   (:truncate #'(lambda (op)
		  (declare (ignore op))
		  'trunc))
   (:acos #'(lambda (op)
	    (declare (ignore op))
	    'acos))
   (:asin #'(lambda (op)
	      (declare (ignore op))
	      'asin))
   (:atan #'(lambda (op)
	      (declare (ignore op))
	      'atan))
   (:cos #'(lambda (op)
	    (declare (ignore op))
	    'cos))
   (:sin #'(lambda (op)
	     (declare (ignore op))
	     'sin))
   (:tan #'(lambda (op)
	     (declare (ignore op))
	     'tan)))

  (arguments
   (form arguments)
   form)
  
  (aggregation (aggregate-function form))
  
  (aggregate-function
   (:avg #'(lambda (op)
	     (declare (ignore op))
	     'avg))
   (:count #'(lambda (op)
	     (declare (ignore op))
	     'count))
   (:every #'(lambda (op)
	     (declare (ignore op))
	     'count))
   (:max #'(lambda (op)
	     (declare (ignore op))
	     'max))
   (:min #'(lambda (op)
	     (declare (ignore op))
	     'min))
   (:sum #'(lambda (op)
	     (declare (ignore op))
	     'max))))

(defmacro expressions ((&rest args) &body body)
  `(values ,(mapcar #'parse-expression (quote body))))

(defun make-root (class-name)
  (make-instance 'root-node :class-mapping (get-class-mapping class-name)))

(defun make-query (roots &key join select where order-by having
			   offset limit fetch mapping-schema)
  (let* ((*table-index* 0)
	 (selectors
	  (if (listp roots)
	      (mapcar #'make-root roots)
	      (list (make-root class-name))))
	 (joined-list
	  (append selectors
		  (when (not (null join))
		    (multiple-value-call #'append
		      (apply join selectors)))))
	 (select-list
	  (apply #'compute-select
		 (if (not (null select))
		     (multiple-value-list
		      (apply select joined-list))
		     selectors))))
    (compute-query select-list
		   (when (not (null where))
		     (multiple-value-list
		      (apply where joined-list)))
		   (when (not (null order-by))
		     (multiple-value-list
		      (apply order-by select-list)))
		   (when (not (null having))
		     (multiple-value-list
		      (apply having joined-list)))
		   (when (not (null fetch))
		     (multiple-value-list
		      (apply fetch fetch-references)))
		   limit offset)))

(defun db-read (roots &key join select where order-by having
			offset limit singlep transform fetch
			(mapping-schema (mapping-schema-of *session*)))
  (declare (ignore transform singlep))
  (let ((query
	 (make-query roots :mapping-schema mapping-schema
		     :join join
		     :select select
		     :where where
		     :order-by order-by
		     :having having
		     :offset offset
		     :limit limit
		     :fetch fetch)))
    (load (execute (query-string query)) query)))


;; выборка подклассов производится только при указании класса объектов
;; в select-list


(defclass query-plan ()
  ((select-list)
   (where-clause)
   (order-by-clause)
   (having-clause)
   (fetch-references)
   (recursive-expressions)
   (limit)
   (offset)))

;;    (setf primary-key (mapcar #'(lambda (column-name)
;;				  (get-column column-name instance))
;;			      (primary-key-of class-mapping)))))

(defun make-expression (&key properties expression count
			  count-from-clause select-list from-clause
			  group-by-clause loader fetch join)
  #'(lambda (context)
      (cond ((eq context :property) properties)
	    ((eq context :expression) expression)
	    ((eq context :count) count)
	    ((eq context :count-from-clause)
	     (or count-from-clause from-clause))
	    ((eq context :select-list) select-list)
	    ((eq context :from-clause) from-clause)
	    ((eq context :group-by-clause) group-by-clause)
	    ((eq context :loader) loader)
	    ((eq context :fetch) fetch)
	    ((eq context :join) join))))

(defun expression-of (expression)
  (if (functionp expression)
      (funcall expression :expression)
      expression))

(defun count-expression-of (expression)
  (funcall expression :count))

(defun count-from-clause-of (expression)
  (funcall expression :count-from-clause))

(defun select-list-of (expression)
  (funcall expression :select-list))

(defun from-clause-of (expression)
  (when (functionp expression)
    (funcall expression :from-clause)))

(defun group-by-clause-of (expression)
  (when (functionp expression)
    (funcall expression :group-by-clause)))

(defun loader (expression)
  (funcall expression :loader))

(defun fetch-references-of (class-mapping)
  (funcall class-mapping :fetch))

(defun join-references-of (class-mapping)
  (funcall class-mapping :join))

(defun make-value-loader (alias)
  #'(lambda (row)
      (rest
       (assoc alias row :test #'string=))))

(defvar *table-index*)

(defun plan-column (table-alias column-name)
  (let ((alias (make-alias column-name))
	(column-expression (list #'write-column column-name table-alias)))
    (values (cons column-expression alias) ;; select item alias))
	    (make-value-loader alias))))

(defun plan-key (table-alias column &rest columns)
  (multiple-value-bind (key-columns key-loader)
      (when (not (null columns))
	(apply #'plan-key table-alias columns))
    (multiple-value-bind (key-column column-loader)
	(plan-column table-alias column)
      (values
       (list* key-column key-columns)
       nil
       #'(lambda (row)
	   (list*
	    (funcall column-loader row)
	    (when (not (null key-loader))
	      (funcall key-loader row))))))))

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

(defun make-property-loader (slot-name column-loader)
  #'(lambda (commited-state row)
      (setf
       (property-value commited-state slot-name)
       (funcall column-loader row))))

(defun plan-properties (alias &optional property &rest properties)
  (when (not (null property))
    (multiple-value-bind (columns loaders)
	(apply #'plan-properties alias properties)
      (destructuring-bind (slot-name column-name column-type)
	  property
	(declare (ignore column-type))
	(multiple-value-bind (column column-loader)
	    (plan-column alias column-name)
	  (values
	   (list* column columns)
	   (list* (make-property-loader slot-name column-loader)
		  loaders)))))))

(defun join-properties (join-path alias
			&optional property &rest properties)
  (when (not (null property))
    (multiple-value-bind (properties columns loaders)
	(apply #'join-properties join-path alias properties)
      (destructuring-bind (slot-name column-name column-type)
	  property
	(declare (ignore column-type))
	(multiple-value-bind (column column-loader)
	    (plan-column alias column-name)
	  (let ((from-clause 
		 (join-path-append
		  (rest join-path)
		  (first join-path))))
	    (values
	     (acons slot-name
		    (make-expression :expression (first column)
				     :count (first column)
				     :select-list (list column)
				     :from-clause from-clause
				     :group-by-clause (list (first column))
				     :loader column-loader)
		    properties)
	     (list* column columns)
	     (list* (make-property-loader slot-name column-loader)
		    loaders))))))))

(defun join-many-to-one (join-path root-alias foreign-key
			 &key class-name table-name primary-key
			   properties one-to-many-mappings
			   many-to-one-mappings inverted-one-to-many
			   superclass-mappings subclass-mappings)
  (let* ((alias (make-alias "join"))
	 (table-join
	  (list #'write-left-join table-name alias
		(mapcar #'(lambda (fk-column pk-column)
			    (list (first fk-column)
				  (first pk-column)))
			(apply #'plan-key root-alias foreign-key)
			(apply #'plan-key alias primary-key)))))
    (plan-class alias class-name table-join join-path primary-key
		properties one-to-many-mappings many-to-one-mappings
		inverted-one-to-many superclass-mappings
		subclass-mappings)))

(defun join-many-to-one-mappings (join-path alias 
				  &optional many-to-one-mapping
				  &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key)
	many-to-one-mapping
      (acons slot-name
	     #'(lambda ()
		 (apply #'join-many-to-one
			join-path alias foreign-key
			(get-class-mapping reference-class-name)))
	     (apply #'join-many-to-one-mappings
		    join-path alias many-to-one-mappings)))))

(defun join-one-to-many (join-path root-primary-key foreign-key
			 class-name &key table-name primary-key
				      properties one-to-many-mappings
				      many-to-one-mappings
				      inverted-one-to-many
				      superclass-mappings
				      subclass-mappings)
  (let* ((alias (make-alias "join"))
	 (table-join
	  (list #'write-left-join table-name alias
		(mapcar #'(lambda (pk-column fk-column)
			    (list (first pk-column)
				  (first fk-column)))
			root-primary-key
			(apply #'plan-key alias foreign-key)))))
    (plan-class alias class-name table-join join-path primary-key
		properties one-to-many-mappings many-to-one-mappings
		inverted-one-to-many superclass-mappings
		subclass-mappings)))

(defun join-one-to-many-mappings (join-path primary-key
				  &optional one-to-many-mapping
				  &rest one-to-many-mappings)
  (when (not (null one-to-many-mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key serializer
				   deserializer)
	one-to-many-mapping
      (declare (ignore serializer deserializer))
      (acons slot-name
	     #'(lambda ()
		 (apply #'join-one-to-many
			join-path primary-key foreign-key
			(get-class-mapping reference-class-name)))
	     (apply #'join-one-to-many-mappings
		    join-path primary-key one-to-many-mappings)))))

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

(defun query-append (query &key select-list from-clause where-clause
			     (group-by-clause nil group-by-present-p)
			     having-clause order-by-clause
			     limit offset)
  (multiple-value-bind (query-select-list
			query-from-clause
			query-where-clause
			query-group-by-clause
			query-having-clause
			query-order-by
			query-limit
			query-offset)
      (when (not (null query))
	(funcall query))
    (let ((select-list
	   (append select-list query-select-list)))
      #'(lambda (&optional expression)
	  (if (not (null expression))
	      (rassoc expression select-list)
	      (values select-list
		      (if (not (null from-clause))
			  (apply #'from-clause-append
				 query-from-clause from-clause)
			  query-from-clause)
		      (if (not (null where-clause))
			  (list* where-clause query-where-clause)
			  query-where-clause)
		      (if (not (null group-by-present-p))
			  (list* group-by-clause query-group-by-clause)
			  query-group-by-clause)
		      (if (not (null having-clause))
			  (list* having-clause query-having-clause)
			  query-having-clause)
		      (if (not (null order-by-clause))
			  (list* order-by-clause query-order-by)
			  query-order-by)
		      (or limit query-limit)
		      (or offset query-offset)))))))

(defun compute-fetch (query loader
		      &optional reference-fetching
		      &rest reference-fetchings)
  (if (not (null reference-fetching))
      (multiple-value-bind (query loader)
	  (funcall reference-fetching query loader)
	(apply #'compute-fetch query loader reference-fetchings))
      (values query loader)))

(defun loader-append (loader reference-loader)
  #'(lambda (row result-set &rest reference-loaders)
      (apply loader row result-set
	     (list* reference-loader
		    reference-loaders))))

(defun compute-inverted-one-to-many-keys (table-alias
					  &optional inverted-one-to-many
					  &rest inverted-one-to-many-mappings)
  (when (not (null inverted-one-to-many))
    (multiple-value-bind (columns key-loaders)
	(apply #'compute-inverted-one-to-many-keys
	       table-alias inverted-one-to-many-mappings)
      (destructuring-bind (&key foreign-key &allow-other-keys)
	  inverted-one-to-many
	(multiple-value-bind (key-columns key-loader)
	    (apply #'plan-key table-alias foreign-key)
	  (values
	   (append key-columns columns)
	   (list* #'(lambda (&rest args)
		      (list* (funcall key-loader args)
			     inverted-one-to-many))
		  key-loaders)))))))

(defun fetch-many-to-one (query loader fetch join-path root-class-name
			  many-to-one-mapping foreign-key-columns 
			  &key class-name table-name primary-key
			    properties one-to-many-mappings
			    many-to-one-mappings inverted-one-to-many
			    superclass-mappings subclass-mappings)
  (let ((alias (make-alias "fetch")))
    (multiple-value-bind (primary-key pk-loader)
	(apply #'plan-key alias primary-key)
      (let ((table-join
	     (list #'write-left-join table-name alias
		   (mapcar #'(lambda (fk-column pk-column)
			       (list (first fk-column)
				     (first pk-column)))
			   foreign-key-columns
			   primary-key))))
	(multiple-value-bind (columns
			      from-clause
			      reference-loader
			      fetch-references)
	    (fetch-object class-name alias table-join join-path
			  primary-key pk-loader properties
			  one-to-many-mappings many-to-one-mappings
			  inverted-one-to-many superclass-mappings
			  subclass-mappings)
	  (apply #'compute-fetch
		 (query-append query
			       :select-list columns
			       :from-clause from-clause
			       :group-by-clause columns)
		 (loader-append loader
				#'(lambda (commited-state object-rows
					   fetched-references)
				    (when (typep (object-of commited-state)
						 root-class-name)
				      (setf
				       (many-to-one-value commited-state
							  many-to-one-mapping)
				       (funcall reference-loader
						(first object-rows)
						object-rows
						fetched-references)))))
		 (when (functionp fetch)
		   (mapcar #'funcall
			   (multiple-value-list
			    (funcall fetch fetch-references))))))))))

(defun fetch-many-to-one-mappings (join-path class-name alias
				   &optional many-to-one-mapping
				   &rest many-to-one-mappings)
  (when (not (null many-to-one-mapping))
    (multiple-value-bind (references columns key-loaders)
	(apply #'fetch-many-to-one-mappings
	       join-path class-name alias many-to-one-mappings)
      (destructuring-bind (slot-name
			   &key reference-class-name foreign-key)
	  many-to-one-mapping
	(multiple-value-bind (foreign-key-columns foreign-key-loader)
	    (apply #'plan-key alias foreign-key)
	  (values
	   (acons slot-name
		  #'(lambda (query loader fetch)
		      (apply #'fetch-many-to-one
			     query loader fetch join-path class-name
			     many-to-one-mapping foreign-key-columns
			     (get-class-mapping reference-class-name)))
		  references)
	   (append foreign-key-columns columns)
	   (list* #'(lambda (commited-state &rest args)
		      (setf (many-to-one-key commited-state
					     many-to-one-mapping)
			    (apply foreign-key-loader
				   commited-state args)))
		  key-loaders)))))))

(defun alias (expression)
  (rest expression))

(defun fetch-one-to-many (query loader fetch join-path root-class-name
			  slot-name root-primary-key foreign-key
			  serializer
			  &key class-name table-name primary-key
			    properties one-to-many-mappings
			    many-to-one-mappings inverted-one-to-many
			    superclass-mappings subclass-mappings)
  (let* ((root-primary-key
	  (reduce #'(lambda (primary-key alias)
		      (list* (funcall query alias) primary-key))
		  root-primary-key :key #'alias :initial-value nil))
	 (alias (make-alias "fetch"))
	 (table-join
	  (list #'write-left-join table-name alias
		(mapcar #'(lambda (pk-column fk-column)
			    (list (first pk-column)
				  (first fk-column)))
			root-primary-key
			(apply #'plan-key alias foreign-key)))))
    (multiple-value-bind (primary-key pk-loader)
	(apply #'plan-key alias primary-key)
      (multiple-value-bind (columns
			    from-clause
			    reference-loader
			    fetch-references)
	  (fetch-object class-name alias table-join join-path
			primary-key pk-loader properties
			one-to-many-mappings many-to-one-mappings
			inverted-one-to-many superclass-mappings
			subclass-mappings)
	(apply #'compute-fetch
	       (query-append query
			     :select-list columns
			     :from-clause from-clause
			     :group-by-clause columns)
	       (loader-append loader
			      #'(lambda (commited-state object-rows
					 fetched-references)
				  (when (typep (object-of commited-state)
					       root-class-name)
				    (setf
				     (one-to-many-value commited-state slot-name)
				     (apply serializer
					    (remove-duplicates
					     (mapcar #'(lambda (row)
							 (funcall reference-loader
								  row
								  object-rows
								  fetched-references))
						     object-rows)))))))
	       (when (functionp fetch)
		 (mapcar #'funcall
			 (multiple-value-list
			  (funcall fetch fetch-references)))))))))

(defun fetch-one-to-many-mappings (join-path class-name primary-key
				   &optional mapping &rest mappings)
  (when (not (null mapping))
    (destructuring-bind (slot-name &key reference-class-name
				   foreign-key serializer
				   deserializer)
	mapping
      (declare (ignore deserializer))
      (acons slot-name
	     #'(lambda (query loader fetch)
		 (apply #'fetch-one-to-many query loader fetch join-path
			class-name slot-name primary-key foreign-key
			serializer
			(get-class-mapping reference-class-name)))
	     (apply #'fetch-one-to-many-mappings
		    join-path class-name primary-key mappings)))))

(defun fetch-slots (class-name alias table-join join-path
		    primary-key-columns primary-key-loader properties
		    one-to-many-mappings many-to-one-mappings
		    inverted-one-to-many superclass-mappings)
  (multiple-value-bind (superclasses-fetched-references
			superclasses-columns
			superclasses-from-clause
			superclasses-loaders)
      (apply #'fetch-superclasses join-path alias superclass-mappings)
    (multiple-value-bind (property-columns property-loaders)
	(apply #'plan-properties alias properties)
      (multiple-value-bind (many-to-one-fetched-references
			    foreign-key-columns foreign-key-loaders)
	  (apply #'fetch-many-to-one-mappings
		 join-path class-name alias many-to-one-mappings)
	(multiple-value-bind (inverted-one-to-many-columns
			      inverted-one-to-many-loaders)
	    (apply #'compute-inverted-one-to-many-keys
		   alias inverted-one-to-many)
	  (values
	   (append many-to-one-fetched-references
		   (apply #'fetch-one-to-many-mappings
			  join-path class-name primary-key-columns
			  one-to-many-mappings)
		   superclasses-fetched-references)
	   (append primary-key-columns
		   property-columns
		   foreign-key-columns
		   inverted-one-to-many-columns
		   superclasses-columns)
	   (append table-join superclasses-from-clause)
	   (list* #'(lambda (commited-state row)
		      (register-object commited-state
				       class-name
				       (funcall primary-key-loader row))
		      (dolist (loader inverted-one-to-many-loaders)
			(funcall loader commited-state row))
		      (dolist (loader foreign-key-loaders)
			(funcall loader commited-state row))
		      (dolist (loader property-loaders commited-state)
			(funcall loader commited-state row)))
		  superclasses-loaders)))))))

(defun fetch-superclass (join-path subclass-alias
			 &key class-name table-name primary-key
			   foreign-key properties one-to-many-mappings
			   many-to-one-mappings inverted-one-to-many
			   superclass-mappings)
  (let* ((alias
	  (make-alias "fetch"))
	 (foreign-key
	  (apply #'plan-key subclass-alias foreign-key)))
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-key alias primary-key)
      (let ((table-join
	     (list #'write-inner-join table-name alias
		   (mapcar #'(lambda (pk-column fk-column)
			       (list (first pk-column)
				     (first fk-column)))
			   primary-key-columns
			   foreign-key))))
	(fetch-slots class-name alias table-join
		     (list* table-join join-path) primary-key-columns
		     primary-key-loader properties
		     one-to-many-mappings many-to-one-mappings
		     inverted-one-to-many superclass-mappings)))))

(defun fetch-superclasses (join-path alias
			   &optional superclass-mapping
			   &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (superclass-fetched-references
			  superclass-columns
			  superclass-from-clause
			  superclass-loaders)
	(apply #'fetch-superclass
	       join-path alias superclass-mapping)
      (multiple-value-bind (superclasses-fetched-references
			    superclasses-columns
			    superclasses-from-clause
			    superclasses-loaders)
	  (apply #'fetch-superclasses
		 join-path alias superclass-mappings)
	(values
	 (append superclass-fetched-references
		 superclasses-fetched-references)
	 (append superclass-columns
		 superclasses-columns)
	 (append superclass-from-clause
		 superclasses-from-clause)
	 (append superclass-loaders
		 superclasses-loaders))))))

;; Carefully implement subclass dynamic association fetching (with
;; class specification)
(defun load-object (class primary-key row
		    superclass-loaders subclass-loaders)
  (let ((commited-state
	 (make-instance 'commited-state
			:object (or (some #'(lambda (loader)
					      (funcall loader row))
					  subclass-loaders)
				    (allocate-instance class)))))
    (dolist (superclass-loader superclass-loaders commited-state)
      (funcall superclass-loader commited-state primary-key row))))

(defun fetch-class (class-name alias table-join join-path
		    primary-key-columns primary-key-loader properties
		    one-to-many-mappings many-to-one-mappings
		    inverted-one-to-many superclass-mappings
		    subclass-mappings)
  (let ((class (find-class class-name)))
    (multiple-value-bind (fetched-references
			  columns from-clause superclass-loaders)
	(fetch-slots class-name alias table-join (list* table-join join-path)
		     primary-key-columns primary-key-loader properties
		     one-to-many-mappings many-to-one-mappings
		     inverted-one-to-many superclass-mappings)
      (multiple-value-bind (subclasses-fetched-references
			    subclasses-columns
			    subclasses-from-clause subclass-loaders)
	  (apply #'fetch-subclasses
		 join-path primary-key-columns subclass-mappings)
	  (values
	   (append fetched-references
		   subclasses-fetched-references)
	   (append columns subclasses-columns)
	   (append from-clause subclasses-from-clause)
	   #'(lambda (row)
	       (load-object class
			    (funcall primary-key-loader row)
			    row
			    superclass-loaders
			    subclass-loaders)))))))

(defun join-path-append (join-path from-clause)
  (reduce #'(lambda (from-clause table-join)
	      (append table-join (list from-clause)))
	  join-path :from-end nil
	  :initial-value from-clause))

(defun fetch-object (class-name alias table-join join-path primary-key
		     primary-key-loader properties
		     one-to-many-mappings many-to-one-mappings
		     commited-state superclass-mappings subclass-mappings)
  (multiple-value-bind (fetched-references
			fetched-columns
			fetched-from-clause class-loader)
      (fetch-class class-name alias table-join join-path
		   primary-key primary-key-loader
		   properties one-to-many-mappings
		   many-to-one-mappings commited-state
		   superclass-mappings subclass-mappings)
    (let ((class-loader
	   #'(lambda (row result-set reference-loaders)
	       (let ((primary-key
		      (funcall primary-key-loader row)))
		 (or
		  (get-object class-name primary-key)
		  (let ((commited-state
			 (funcall class-loader row)))
		    (dolist (loader reference-loaders
			     (object-of commited-state))
		      (funcall loader
			       commited-state
			       (remove primary-key result-set
				       :key primary-key-loader
				       :test-not #'equal))))))))
	  (path (join-path-append join-path fetched-from-clause)))
      (values fetched-columns
	      path
	      class-loader
	      #'(lambda (reader)
		  (values (rest
			   (assoc (get-slot-name (find-class class-name)
						 reader)
				  fetched-references))
			  class-loader))))))

(defun fetch-subclass (join-path superclass-primary-key 
		       &key class-name primary-key table-name
			 foreign-key properties one-to-many-mappings
			 many-to-one-mappings inverted-one-to-many
			 superclass-mappings subclass-mappings)
  (let* ((alias (make-alias "fetch"))
	 (foreign-key (apply #'plan-key alias foreign-key))
	 (table-join
	  (list #'write-left-join table-name alias
		(mapcar #'(lambda (pk-column fk-column)
			    (list (first pk-column)
				  (first fk-column)))
			superclass-primary-key
			foreign-key))))
    (multiple-value-bind (primary-key-columns primary-key-loader)
	(apply #'plan-key alias primary-key)
      (multiple-value-bind (class-fetched-references
			    class-columns
			    class-from-clause
			    class-loader)
	  (fetch-class class-name alias table-join join-path
		       primary-key-columns primary-key-loader
		       properties one-to-many-mappings
		       many-to-one-mappings inverted-one-to-many
		       superclass-mappings subclass-mappings)
	(values class-fetched-references
		class-columns
		class-from-clause
		#'(lambda (row)
		    (let ((primary-key
			   (funcall primary-key-loader row)))
		      (when (notevery #'null primary-key)
			(funcall class-loader primary-key row)))))))))

(defun fetch-subclasses (join-path superclass-primary-key
			 &optional subclass-mapping
			 &rest subclass-mappings)
  (when (not (null subclass-mapping))
    (multiple-value-bind (subclass-fetched-references
			  subclass-columns
			  subclass-from-clause
			  subclass-loader)
	(apply #'fetch-subclass
	       join-path superclass-primary-key subclass-mapping)
      (multiple-value-bind (subclasses-fetched-references
			    subclasses-columns
			    subclasses-from-clause
			    subclasses-loaders)
	  (apply #'fetch-subclasses
		 join-path superclass-primary-key subclass-mappings)
	(values
	 (append subclass-fetched-references
		 subclasses-fetched-references)
	 (append subclass-columns
		 subclasses-columns)
	 (list* subclass-from-clause
		subclasses-from-clause)
	 (list* subclass-loader
		subclasses-loaders))))))

(defun join-superclass (subclass-alias join-path
			&key class-name table-name primary-key
			  foreign-key properties one-to-many-mappings
			  many-to-one-mappings inverted-one-to-many
			  superclass-mappings)
  (let ((alias
	 (make-alias "join"))
	(foreign-key
	 (apply #'plan-key subclass-alias foreign-key)))
    (multiple-value-bind (primary-key primary-key-loader)
	(apply #'plan-key alias primary-key)
      (let ((table-join
	     (list #'write-inner-join table-name alias
		   (mapcar #'(lambda (pk-column fk-column)
			       (list
				(first pk-column)
				(first fk-column)))
			   primary-key
			   foreign-key))))
	(join-class alias table-join (list* table-join join-path)
		    class-name primary-key primary-key-loader
		    properties one-to-many-mappings
		    many-to-one-mappings inverted-one-to-many
		    superclass-mappings)))))

(defun join-superclasses (alias join-path
			  &optional superclass-mapping
			  &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (superclasses-properties
			  superclasses-joined-references
			  superclasses-fetched-references
			  superclasses-columns
			  superclasses-from-clause
			  superclasses-loaders)
	(apply #'join-superclasses alias
	       join-path superclass-mappings)
      (multiple-value-bind (superclass-properties
			    superclass-joined-references
			    superclass-fetched-references
			    superclass-columns
			    superclass-from-clause
			    superclass-loaders)
	  (apply #'join-superclass alias
		 join-path superclass-mapping)
	(values
	 (append superclass-properties
		 superclasses-properties)
	 (append superclass-joined-references
		 superclasses-joined-references)
	 (append superclass-fetched-references
		 superclasses-fetched-references)
	 (append superclass-columns
		 superclasses-columns)
	 (list* superclass-from-clause
		superclasses-from-clause)
	 (append superclass-loaders
		 superclasses-loaders))))))

(defun join-class (alias table-join join-path class-name
		   primary-key-columns primary-key-loader properties
		   one-to-many-mappings many-to-one-mappings
		   inverted-one-to-many superclass-mappings)
  (multiple-value-bind (superclasses-properties
			superclasses-joined-references
			superclasses-fetched-references
			superclasses-columns
			superclasses-from-clause
			superclasses-loaders)
      (apply #'join-superclasses alias join-path superclass-mappings)
    (multiple-value-bind (properties property-columns property-loaders)
	(apply #'join-properties join-path alias properties)
      (multiple-value-bind (many-to-one-fetched-references
			    foreign-key-columns
			    foreign-key-loaders)
	  (apply #'fetch-many-to-one-mappings
		 join-path class-name alias many-to-one-mappings)
	(multiple-value-bind (inverted-one-to-many-columns
			      inverted-one-to-many-loaders)
	    (apply #'compute-inverted-one-to-many-keys
		   alias inverted-one-to-many)
	  (values
	   (append properties
		   superclasses-properties)
	   (append (apply #'join-many-to-one-mappings
			  join-path alias many-to-one-mappings)
		   (apply #'join-one-to-many-mappings join-path
			  primary-key-columns one-to-many-mappings)
		   superclasses-joined-references)
	   (append many-to-one-fetched-references
		   (apply #'fetch-one-to-many-mappings
			  join-path class-name primary-key-columns
			  one-to-many-mappings)
		   superclasses-fetched-references)
	   (append primary-key-columns
		   property-columns
		   foreign-key-columns
		   inverted-one-to-many-columns
		   superclasses-columns)
	   (append table-join superclasses-from-clause)
	   (list* #'(lambda (commited-state row)
		      (register-object commited-state
				       class-name
				       (funcall primary-key-loader row))
		      (dolist (loader inverted-one-to-many-loaders)
			(funcall loader commited-state row))
		      (dolist (loader foreign-key-loaders)
			(funcall loader commited-state row))
		      (dolist (loader property-loaders commited-state)
			(funcall loader commited-state row)))
		  superclasses-loaders)))))))

(defun plan-class (alias class-name table-join join-path primary-key
		   properties one-to-many-mappings
		   many-to-one-mappings inverted-one-to-many
		   superclass-mappings subclass-mappings)
  (multiple-value-bind (primary-key primary-key-loader)
      (apply #'plan-key alias primary-key)
    (multiple-value-bind (joined-properties
			  joined-references
			  fetched-references
			  fetched-columns
			  fetched-from-clause
			  superclasses-loaders)
	(join-class alias table-join (list* table-join join-path)
		    class-name primary-key primary-key-loader
		    properties one-to-many-mappings
		    many-to-one-mappings inverted-one-to-many
		    superclass-mappings)
      (multiple-value-bind (subclasses-fetch-references
			    subclasses-columns
			    subclasses-from-clause
			    subclasses-loaders)
	  (apply #'fetch-subclasses (list* table-join join-path)
		 primary-key subclass-mappings)
	(let* ((class (find-class class-name))
	       (class-loader
		#'(lambda (row result-set reference-loaders)
		    (let ((primary-key
			   (funcall primary-key-loader row)))
		      (or
		       (get-object class-name primary-key)
		       (let ((commited-state
			      (load-object class primary-key row
					   superclasses-loaders
					   subclasses-loaders)))
			 (dolist (loader reference-loaders
				  (object-of commited-state))
			   (funcall loader
				    commited-state
				    (remove primary-key result-set
					    :key primary-key-loader
					    :test-not #'equal))))))))
	       (columns (append fetched-columns
				subclasses-columns))
	       (path (join-path-append join-path table-join))
	       (from-clause
		(join-path-append join-path
				  (append fetched-from-clause
					  subclasses-from-clause)))
	       (fetch-references
		(append fetched-references
			subclasses-fetch-references)))
	  (values (make-expression :properties
				   #'(lambda (reader)
				       (rest
					(assoc (get-slot-name class reader)
					       joined-properties)))
				   :select-list columns
				   :count (mapcar #'first primary-key)
				   :count-from-clause path
				   :from-clause from-clause
				   :group-by-clause (mapcar #'first columns)
				   :loader class-loader
				   :fetch
				   #'(lambda (reader)
				       (values (rest
						(assoc
						 (get-slot-name (find-class class-name)
								reader)
						 fetch-references))
					       class-loader))
				   :join #'(lambda (reader)
					     (funcall
					      (rest
					       (assoc (get-slot-name class reader)
						      joined-references)))))))))))

(defun plan-root-class-mapping (&key class-name table-name primary-key
				properties one-to-many-mappings
				many-to-one-mappings inverted-one-to-many
				superclass-mappings subclass-mappings)
  (let ((alias (make-alias "root")))
    (plan-class alias class-name
		(list #'write-table-reference table-name alias)
		nil primary-key properties one-to-many-mappings
		many-to-one-mappings inverted-one-to-many
		superclass-mappings subclass-mappings)))

(defun make-join-plan (mapping-schema class-name &rest class-names)
  (list* (apply #'plan-root-class-mapping
		(get-class-mapping class-name mapping-schema))
	 (reduce #'(lambda (class-name result)
		     (list* (apply #'plan-root-class-mapping
				   (get-class-mapping class-name mapping-schema))
			    result))
		 class-names
		 :from-end t
		 :initial-value nil)))
