(in-package #:cl-db)

;; Query root

(defclass class-node ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (superclass-nodes :reader superclass-nodes-of)
   (direct-references :reader direct-references-of)
   (alias :initarg :alias
	  :reader table-alias-of) ; возможно его нужно определить в элементе select-list
   (path :initarg :path
	 :reader path-of)))

(defclass superclass-node (class-node)
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass concrete-class-node (class-node)
  ((inheritance-nodes :reader inheritance-nodes-of)))

(defclass root-node (concrete-class-node)
  ((properties :reader properties-of)
   (effective-references :reader effective-references-of)))

(defclass reference-node (root-node) ;; for joins
  ((reference :initarg :reference
	      :reader reference-of)
   (parent-node :initarg :parent-node
		:reader parent-node-of)))

(defclass property-selection ()
  ((property-mapping :initarg :property-mapping
		     :reader property-mapping-of)
   (class-node :initarg :class-node
	       :reader class-node-of)))

(defclass class-selection () ;; (root-node-selection)
  ((subclass-selections :reader subclass-selections-of)
   (concrete-class-node :initarg :concrete-class-node
			:reader concrete-class-node-of)))

(defclass root-class-selection (class-selection)
  ((references :reader references-of))) ;; references with superclass references

(defclass subclass-selection (class-selection)
  ())

(defclass reference-fetching (class-selection)
  ((reference :initarg :reference
	      :reader reference-of)
   (recursive-selection :initarg :recursive-selection
			:reader recursive-selection-of)
   (fetched-references :reader fetched-references-of)
   (class-selection :initarg :class-selection
		    :reader class-selection-of)))

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

(defclass rdbms-function-call (sql-function) ;; call by name
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

(defclass recursive-class-node ()
  ((common-table-expression :initarg :common-table-expression
			    :reader common-table-expression-of)
   (class-node :initarg :class-node
	       :reader class-node-of)))

(defclass expression-selection ()
  ((alias :initarg :alias
	  :reader alias-of)
   (expression :initarg :expression
	       :reader expression-of)))

;;; Class selection

;;(defclass select-item () ; i.e. for sql expressions, property selection, columns etc.
;;  ((expressions :initarg :expressions
;;		:reader expressions-of)
;;   (from-clause :initarg :from-clause
;;		:reader from-clause-of)))

;;(defclass root-node-selection (select-item)
;;  ((root-node :initarg :root-node
;;	      :reader root-node-of)))

(defclass joining ()
  ((roots :initarg :roots
	  :reader roots-of)
   (join-list :initarg :join-list
	      :reader join-list-of)
   (aux-clause :initarg :aux-clause
	       :reader aux-clause)))

(defclass selection ()
  ((joining :initarg :joining
	    :reader joining-of)
   (select-list :initarg :select-list
		:accessor select-list-of)
   (where-clause :initarg :where-clause
		 :reader where-clause-of)
   (having-clause :initarg :having-clause
		  :reader having-clause-of)
   (limit :initarg :limit
	  :accessor limit-of)
   (offset :initarg :offset
	   :accessor offset-of)
   (order-by-clause :initarg :order-by-clause
		    :accessor order-by-clause-of)))

(defclass fetching ()
  ((selection :initarg :selection
	      :reader selection-of)
   (fetch-clause :initarg :fetch-clause
		 :reader fetch-clause-of)))

(defclass common-table-expression ()
  ((name :initarg :name
	 :reader name-of)
   (query :initarg :query
	  :reader query-of)
   (recursive-clause :initarg :recursive-clause
		     :reader recursive-clause-of)))

(defclass sql-query ()
  ((select-list)
   (from-clause)
   (where-clause)
   (group-by-clause)
   (having-clause)
   (order-by)
   (limit)
   (offset)))

(defclass column-plan ()
  ((column-name :initarg :column-name
		:reader column-name-of)
   (class-node :initarg :class-node
	       :reader class-node-of)))
