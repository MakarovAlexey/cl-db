(in-package #:cl-db)

(defclass expression ()
  ())

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

(defclass operation (expression)
  ((operator :initarg :operator :reader operator-of)))

(defclass binary-operation (operation)
  ((lhs-expression :initarg :lhs :reader lhs-expression-of)
   (rhs-expression :initarg :rhs :reader rhs-expression-of)))

(defclass binary-operator-extended (operation)
  ((args :initarg :args
	 :reader args-of)))

(defclass sort-direction (operation)
  ())

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
