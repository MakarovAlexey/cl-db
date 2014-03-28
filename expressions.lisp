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

;;(defmacro define-sql-operator (name sql-name)
;;  `(setf (gethash (quote ,name) *sql-expression-types*)
;;	 (make-instance 'sql-operator :sql-string ,sql-name)))

;;(defmacro define-sql-function (name sql-name)
;;  `(setf (gethash (quote ,name) *sql-expression-types*)
;;	 (make-instance 'sql-function :sql-string ,sql-name)))

;;(define-binary-operator (:eq :=) "=")

;;(define-binary-operator :or "OR")

;;(define-binary-operator :and "AND")

;;(define-operator :not "NOT")

;;(define-operator :in "IN")

;;(define-operator :like "LIKE")

;;(define-operator :ilike "ILIKE")

;;(define-operator :similar "SIMILAR")

;;(define-operator :< "<")

;;(define-operator :> ">")

;;(define-operator :<= "<=")

;;(define-operator :>= ">=")

;;(define-operator :<> "<>")

;;(define-operator :!= "!=")

;;(define-operator :between "BETWEEN")

;;(define-operator :+ "+")

;;(define-operator :- "-")

;;(define-operator :* "*")

;;(define-operator :/ "/")

;;(define-operator :% "%")

;;(define-operator :expt "^")

;; (define-aggregate-function :count "count")

