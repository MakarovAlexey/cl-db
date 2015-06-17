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

;; Logical operators

(defun db-operator (operator expression &rest rest-expressions)
  (list* 
  (multiple-value-bind (expression from-clause group-by-clause)
      (funcall expression)
    (multiple-value-bind (rest-expressions
			  rest-from-clause rest-group-by-clause)
	(when (not (null rest-expressions))
	  (apply #'db-and rest-expressions))
      (let ((alias (make-alias "expr")))
	#'(lambda ()
	    (values
	     (list* operator expression rest-expressions)
	     (append from-clause rest-from-clause)
	     (list* group-by-clause 
	     (make-simple-value-loader alias)
	     (list alias)))))))

(defmacro define-binary-operator (function-name operator)
  `(defun ,fucntion-name (lhs-expression rhs-expression &rest expressions)
     (list* ,operator lhs-expression rhs-expression &rest expressions)))

(defun db-and (expression &rest rest-expressions)
  (:documentation "Logical AND operator")
  (apply #'db-operator :and expression rest-expressions))

(defun db-or (expression &rest rest-expressions)
  (:documentation "Logical OR operator")
  (apply #'db-operator :or expression rest-expressions))

(defun db-not (expression &rest rest-expressions)
  (:documentation "Logical NOT operator")
  (apply #'db-operator :not expression rest-expressions))

;; Comparison operators

(defun db-less-than (first-expression second-expression
		     &rest rest-expressions)
  (:documentation "Less than")
  (let ((expression
	 (apply #'db-expression :< first-expression second-expression)))
    (reduce #'(lambda (result expression)
		(
;;    (if (not (null rest-expressions))
    (db-and expression (db-less-than second-expression
				      rest-expressions))
	expression)))
	(db-expression :< 

< 	less than
> 	greater than
<= 	less than or equal to
>= 	greater than or equal to
= 	equal
<> or != 	not equal

(defun db-eq (value-1 value-2)
  (multiple-value-bind (column-1 from-clause-1 loader-1)
      (funcall value-1)
    (multiple-value-bind (column-2 from-clause-2 loader-3)
	(funcall value-2)
      (values (list := 
      


  (let ((property (funcall object accesor)))
    #'(lambda ()
	(multiple-value-bind (column from-clause loader)
	    (funcall property)
	  (values (list '= column '$?)
		  from-clause
		  loader
		  value)))))
