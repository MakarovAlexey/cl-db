(in-package #:cl-db)

(defclass expression ()
  ())

(defclass binary-operation (expression)
  ((lhs-expression :initarg :lhs :reader lhs-expression-of)
   (rhs-expression :initarg :rhs :reader rhs-expression-of)))

(defclass n-ary-expression (expression)
  ((arguments :initarg :arguments
	      :reader arguments-of)))

(defclass sql-function (n-ary-expression)
  ())

(defclass rdbms-sql-function (sql-function)
  ((name :initarg :name :reader name-of)))

(defclass aggregation (sql-function)
  ())

(defclass rdbms-aggeregation (aggregation)
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
  ((class-node :initarg :class-node
	       :reader class-node-of)))

(defun recursive (class-node)
  (make-instance 'recursive-class-node :class-node class-node))

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
