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

(defun db-logical-oexpression (operator expression &rest rest-expressions)
  (multiple-value-bind (expression from-clause loader alias)
      (funcall expression)
    (declare (ignore loader alias))
    (multiple-value-bind (rest-expressions rest-from-clause
			  rest-loaders rest-alias)
	(when (not (null rest-expressions))
	  (apply #'db-and rest-expressions))
      (declare (ignore rest-loaders rest-alias))
      (let ((alias (make-alias "column")))
	#'(lambda ()
	    (values
	     (list* operator expression rest-expressions)
	     (append from-clause rest-from-clause)
	     (make-simple-value-loader alias)
	     (list alias)))))))

(defun db-and (expression &rest rest-expressions)
  (:documentation "Logical AND operator")
  (apply #'db-logical-oexpression :and expression rest-expressions))

(defun db-or (expression &rest rest-expressions)
  (:documentation "Logical OR operator")
  (apply #'db-logical-oexpression :or expression rest-expressions))

(defun db-not (expression &rest rest-expressions)
  (:documentation "Logical NOT operator")
  (apply #'db-logical-oexpression :not expression rest-expressions))



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
