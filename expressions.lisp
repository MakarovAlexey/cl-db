(in-package #:cl-db)

(defparameter *sql-expression-types* (make-hash-table))

(defclass sql-expression-type ()
  ((sql-string :initarg :sql-string
	       :reader sql-string-of)))

(defclass sql-operator (sql-expression-type)
  ())

(defclass sql-function (sql-expression-type)
  ())

(defun expression (name &rest args)
  (multiple-value-bind (expression-type presentp)
      (gethash name *sql-expression-types*)
    (if (not presentp)
	(error "Expression type ~a not found" name)
	(list* expression-type args))))

(defmacro define-sql-operator (name sql-name)
  `(setf (gethash (quote ,name) *sql-expression-types*)
	 (make-instance 'sql-operator :sql-string sql-string)))

(defmacro define-sql-function (name sql-name)
  `(setf (gethash (quote ,name) *sql-expression-types*)
	 (make-instance 'sql-function :sql-string sql-string)))

(define-operator (:eq obj1 obj2)
    stream
  (format stream "~a IS ~a" obj1 obj2))

(define-operator (:or &rest expressions)
    stream
  (format stream "\{~a OR ~a\}" obj1 obj2))

(define-infix-operator (:and &rest expressions)
    stream
  "AND")

(define-prefix-operator :not "NOT")

(define-infix-operator (:in expression &rest values)
    "IN")

(define-infix-operator :in "LIKE")

(define-infix-operator :in "ILIKE")

(define-infix-operator :in "SIMILAR")

(define-infix-operator := "=")

(define-infix-operator :< "<")

(define-infix-operator :> ">")

(define-infix-operator :<= "<=")

(define-infix-operator :>= ">=")

(define-infix-operator :<> "<>")

(define-infix-operator :!= "!=")

(define-infix-operator :between "BETWEEN")

(define-infix-operator :+ "+")

(define-infix-operator :- "-")

(define-infix-operator :* "*")

(define-infix-operator :/ "/")

(define-infix-operator :% "%")

(define-infix-operator :expt "^")

(define-infix-operator :^ "^")



;; (define-aggregate-function :count "count")

